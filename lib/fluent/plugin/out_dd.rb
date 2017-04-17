require 'fluent/plugin/output'

class Fluent::Plugin::DdOutput < Fluent::Plugin::Output
  Fluent::Plugin.register_output('dd', self)

  helpers :compat_parameters

  DEFAULT_BUFFER_TYPE = "memory"

  unless method_defined?(:log)
    define_method('log') { $log }
  end

  config_param :dd_api_key, :string, :secret => true
  config_param :dd_app_key, :string, :default => nil, :secret => true
  config_param :host, :string, :default => nil
  config_param :device, :string, :default => nil
  config_param :silent, :bool, :default => true
  config_param :timeout, :integer, :default => nil
  config_param :use_fluentd_tag_for_datadog_tag, :bool, :default => false
  config_param :emit_in_background, :bool, :default => false
  config_param :concurrency, :integer, :default => nil

  config_section :buffer do
    config_set_default :@type, DEFAULT_BUFFER_TYPE
    config_set_default :chunk_keys, ['tag']
  end

  def initialize
    super
    require 'dogapi'
    require 'socket'
    require 'thread'
  end

  def start
    super

    if @emit_in_background
      @queue = Queue.new

      @threads = @concurrency.times.map do
        Thread.start do
          while (job = @queue.pop)
            emit_points(*job)
            Thread.pass
          end
        end
      end
    end
  end

  def shutdown
    super

    if @emit_in_background
      @threads.size.times do
        @queue.push(false)
      end
      @threads.each do |thread|
        thread.join
      end
    end
  end

  def configure(conf)
    compat_parameters_convert(conf, :buffer)
    super

    unless @dd_api_key
      raise Fluent::ConfigError, '`dd_api_key` is required'
    end

    if !@emit_in_background && @concurrency
      raise Fluent::ConfigError, '`concurrency` should be used with `emit_in_background`'
    end
    @concurrency ||= 1

    unless @host
      @host = %x[hostname -f 2> /dev/null].strip
      @host = Socket.gethostname if @host.empty?
    end

    @dog = Dogapi::Client.new(@dd_api_key, @dd_app_key, @host, @device, @silent, @timeout)
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def formatted_to_msgpack_binary
    true
  end

  def write(chunk)
    jobs = chunk_to_jobs(chunk)

    jobs.each do |job|
      if @emit_in_background
        @queue.push(job)
      else
        emit_points(*job)
      end
    end
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
