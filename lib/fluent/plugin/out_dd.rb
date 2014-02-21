class Fluent::DdOutput < Fluent::BufferedOutput
  include Fluent::SetTimeKeyMixin
  include Fluent::SetTagKeyMixin

  Fluent::Plugin.register_output('dd', self)

  unless method_defined?(:log)
    define_method('log') { $log }
  end

  config_set_default :include_time_key, true
  config_set_default :include_tag_key, true

  config_param :dd_api_key, :string
  config_param :host, :string, :default => nil

  def initialize
    super
    require 'dogapi'
    require 'socket'
    require 'time'
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
    record.to_msgpack
  end

  def write(chunk)
    enum = chunk.to_enum(:msgpack_each)

    enum.select {|record|
      unless record['metric']
        log.warn("`metric` key does not exist: #{record.inspect}")
      end

      record['metric']
    }.chunk {|record|
      record.values_at('metric', 'tag', 'host', 'type')
    }.each {|i, records|
      metric, tag, host, type = i

      points = records.map do |record|
        time = Time.parse(record['time'])
        value = record['value']
        [time, value]
      end

      options = {}
      options['tags'] = [tag] if tag
      options['host'] = host if host
      options['type'] = type if type

      @dog.emit_points(metric, points, options)
    }
  end
end
