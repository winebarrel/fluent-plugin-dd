class Fluent::DdOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output('dd', self)

  unless method_defined?(:log)
    define_method('log') { $log }
  end

  config_param :dd_api_key, :string
  config_param :host, :string, :default => nil
  config_param :use_fluentd_tag_for_datadog_tag, :bool, :default => false

  def initialize
    super
    require 'dogapi'
    require 'socket'
  end

  def start
    super
  end

  def shutdown
    super
  end

  def configure(conf)
    super

    unless @dd_api_key
      raise Fluent::ConfigError, '`dd_api_key` is required'
    end

    unless @host
      @host = %x[hostname -f 2> /dev/null].strip
      @host = Socket.gethostname if @host.empty?
    end

    @dog = Dogapi::Client.new(@dd_api_key, nil, @host)
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
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

      [dd_tag] + record.values_at('metric', 'host', 'type')
    }.each {|i, records|
      tag, metric, host, type = i

      points = records.map do |tag, time, record|
        time = Time.at(time)
        value = record['value']
        [time, value]
      end

      options = {}
      options[:tags] = tag.split(',').map {|i| i.strip } if tag
      options[:host] = host if host
      options[:type] = type if type

      @dog.emit_points(metric, points, options)
    }
  end
end
