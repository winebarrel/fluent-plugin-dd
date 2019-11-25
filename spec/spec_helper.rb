require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'fluent/plugin/out_dd'
require 'dogapi'
require 'time'

# Disable Test::Unit
module Test::Unit::RunCount; def run(*); end; end
Test::Unit.run = true if defined?(Test::Unit) && Test::Unit.respond_to?(:run=)

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
dd_api_key #{dd_api_key}
host #{host}
#{additional_options}
  EOS

  tag = options[:tag] || 'test.default'
  driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::DdOutput).configure(fluentd_conf)

  driver.run(default_tag: tag, wait_flush_completion: false, shutdown: false) do
    dog = driver.instance.instance_variable_get(:@dog)
    yield(driver, dog)
  end
end
