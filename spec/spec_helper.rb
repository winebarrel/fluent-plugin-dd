require 'fluent/test'
require 'fluent/plugin/out_dd'
require 'dogapi'
require 'time'

# Disable Test::Unit
module Test::Unit::RunCount; def run(*); end; end

RSpec.configure do |config|
  config.before(:all) do
    Fluent::Test.setup
  end
end

def run_driver(options = {})
  options = options.dup

  dd_api_key = options[:dd_api_key] || 'test_dd_api_key'
  host = options[:host] || 'test_host'

  fluentd_conf = <<-EOS
type datadog
dd_api_key #{dd_api_key}
host #{host}
  EOS

  tag = options[:tag] || 'test.default'
  driver = Fluent::Test::OutputTestDriver.new(Fluent::DdOutput, tag).configure(fluentd_conf)

  driver.run do
    dog = driver.instance.instance_variable_get(:@dog)
    yield(driver, dog)
  end
end
