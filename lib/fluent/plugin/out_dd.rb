require 'dogapi'
require 'socket'
require 'fluent/plugin/output'

class Fluent::Plugin::DdOutput < Fluent::Plugin::Output
  Fluent::Plugin.register_output('dd', self)

  helpers :compat_parameters, :timer

  DEFAULT_BUFFER_TYPE = "memory"

  config_param :dd_api_key, :string, secret: true
  config_param :dd_app_key, :string, default: nil, secret: true
  config_param :host, :string, default: nil
  config_param :device, :string, default: nil
  config_param :silent, :bool, default: true
  config_param :timeout, :integer, default: nil
  config_param :use_fluentd_tag_for_datadog_tag, :bool, default: false
  config_param :wait_job_interval, :time, default: 3

  config_section :buffer do
    config_set_default :@type, DEFAULT_BUFFER_TYPE
    config_set_default :chunk_keys, ['tag']
  end

  def configure(conf)
    compat_parameters_convert(conf, :buffer)
    super

    unless @host
      @host = %x[hostname -f 2> /dev/null].strip
      @host = Socket.gethostname if @host.empty?
    end

    @dog = Dogapi::Client.new(@dd_api_key, @dd_app_key, @host, @device, @silent, @timeout)
    @waiting_ids_mutex = Mutex.new
    @waiting_ids = []
  end

  def start
    super

    timer_execute(:out_dd_commit_write, @wait_job_interval) do
      @waiting_ids_mutex.synchronize { @waiting_ids.dup }.each do |chunk_id|
        commit_write(chunk_id)
      end
    end
  end

  def multi_workers_ready?
    true
  end

  def prefer_delayed_commit
    true
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def formatted_to_msgpack_binary
    true
  end

  def try_write(chunk)
    jobs = chunk_to_jobs(chunk)
    chunk_id = chunk.unique_id

    jobs.each do |job|
      emit_points(*job)
    end
    @waiting_ids_mutex.synchronize { @waiting_ids << chunk_id }
  end

  private

  def emit_points(metric, points, options)
    @dog.emit_points(metric, points, options)
  end

  def chunk_to_jobs(chunk)
    enum = chunk.to_enum(:msgpack_each)

    enum.select {|tag, time, record|
      unless record['metric']
        log.warn("`metric` key does not exist: #{[tag, time, record].inspect}")
      end

      record['metric']
    }.chunk {|tag, time, record|
      dd_tag = record['tag']

      if not dd_tag and @use_fluentd_tag_for_datadog_tag
        dd_tag = tag
      end

      [dd_tag] + record.values_at('metric', 'host', 'type', 'device')
    }.map {|i, records|
      tag, metric, host, type, device = i

      points = records.map do |tag, time, record|
        time = Time.at(time)
        value = record['value']
        [time, value]
      end

      options = {}
      options[:tags] = tag.split(',').map {|i| i.strip } if tag
      options[:host] = host if host
      options[:type] = type if type
      options[:device] = device if device

      [metric, points, options]
    }
  end
end
