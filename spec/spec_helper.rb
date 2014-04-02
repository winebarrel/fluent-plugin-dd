require 'fluent/test'
require 'fluent/plugin/out_dd'
require 'dogapi'
require 'time'

# Disable Test::Unit
module Test::Unit::RunCount; def run(*); end; end

class Fluent::DdOutput < Fluent::BufferedOutput
  private
  alias_method :orig_emit_points, :emit_points

  def emit_points(*args)
    if $threads_array_for_test
      $threads_array_for_test << Thread.current
    end
    orig_emit_points(*args)
  end
end

RSpec.configure do |config|
  config.before(:all) do
    Fluent::Test.setup
  end
end

def run_driver(options = {})
  options = options.dup

  dd_api_key = options.delete(:dd_api_key) || 'test_dd_api_key'
  host = options.delete(:host) || 'test_host'

  additional_options = options.map {|key, value|
    "#{key} #{value}"
  }.join("\n")

  fluentd_conf = <<-EOS
type datadog
dd_api_key #{dd_api_key}
host #{host}
#{additional_options}
  EOS

  tag = options[:tag] || 'test.default'
  driver = Fluent::Test::OutputTestDriver.new(Fluent::DdOutput, tag).configure(fluentd_conf)

  driver.run do
    dog = driver.instance.instance_variable_get(:@dog)
    yield(driver, dog)
  end
end
