describe Fluent::Plugin::DdOutput do
  include Fluent::Test::Helpers

  let(:time) {
    event_time('2014-02-08 04:14:15 UTC')
  }

  it 'should load config' do
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::DdOutput)

    driver.configure(<<-EOS)
      type dd
      dd_api_key API_KEY
      dd_app_key APP_KEY
      host my_host.example.com
      device my_device
      silent false
      timeout 5
      use_fluentd_tag_for_datadog_tag true
      emit_in_background true
    EOS

    expect(driver.instance.dd_api_key).to eq 'API_KEY'
    expect(driver.instance.dd_app_key).to eq 'APP_KEY'
    expect(driver.instance.host).to eq 'my_host.example.com'
    expect(driver.instance.device).to eq 'my_device'
    expect(driver.instance.silent).to eq false
    expect(driver.instance.timeout).to eq 5
    expect(driver.instance.use_fluentd_tag_for_datadog_tag).to eq true
    expect(driver.instance.emit_in_background).to eq true
  end

  it 'should receive an API key' do
    expect(Dogapi::Client).to receive(:new).with("test_dd_api_key", nil, "test_host", nil, true, nil)
    run_driver {|d, dog| }
  end

  it 'should be called emit_points' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0})
    end
  end

  context 'with emit_in_background' do
    it 'should be called emit_points in the background' do
      run_driver(:emit_in_background => true) do |d, dog|
        expect(dog).to receive(:emit_points).with(
          "some.metric.name",
          [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
           [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
          {}
        )

        d.feed(time, {"metric" => "some.metric.name", "value" => 50.0})
        d.feed(time, {"metric" => "some.metric.name", "value" => 100.0})
      end
    end

    it 'should be called emit_points in the background and in the different thread' do
      run_driver(
        :emit_in_background => true,
        :concurrency => 2,
      ) do |d, dog|
        expect(dog).to receive(:emit_points).with(
          "some.metric.name.1",
          [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0]],
          {}
        )
        expect(dog).to receive(:emit_points).with(
          "some.metric.name.2",
          [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
          {}
        )

        d.feed(time, {"metric" => "some.metric.name.1", "value" => 50.0})
        d.feed(time, {"metric" => "some.metric.name.2", "value" => 100.0})
      end
    end
  end

  it 'should be called emit_points with tag' do
    run_driver(:use_fluentd_tag_for_datadog_tag => true) do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {:tags=>["test.default"]}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0})
    end
  end

  it 'should be called emit_points for each tag' do
    run_driver(:use_fluentd_tag_for_datadog_tag => true, :tag => 'test.1') do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {:tags=>["test.1"]}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0})
    end

    run_driver(:use_fluentd_tag_for_datadog_tag => true, :tag => 'test.2') do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 150.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 200.0]],
        {:tags=>["test.2"]}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 150.0})
      d.feed(time, {"metric" => "some.metric.name", "value" => 200.0})
    end

    run_driver(:use_fluentd_tag_for_datadog_tag => true, :tag => 'test.3') do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 250.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 300.0]],
        {:tags=>["test.3"]}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 250.0})
      d.feed(time, {"metric" => "some.metric.name", "value" => 300.0})
    end
  end

  it 'should be called emit_points for each tag (tag is included in the record)' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {:tags=>["test.11"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 150.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 200.0]],
        {:tags=>["test.21"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 250.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 300.0]],
        {:tags=>["test.31"]}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0,  "tag" => "test.11"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0, "tag" => "test.11"})

      d.feed(time, {"metric" => "some.metric.name", "value" => 150.0, "tag" => "test.21"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 200.0, "tag" => "test.21"})

      d.feed(time, {"metric" => "some.metric.name", "value" => 250.0, "tag" => "test.31"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 300.0, "tag" => "test.31"})
    end
  end

  it 'should be called emit_points with multiple tags' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {:tags=>["test.12","test.13"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 150.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 200.0]],
        {:tags=>["test.22","test.23"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 250.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 300.0]],
        {:tags=>["test.32","test.33"]}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0,  "tag" => "test.12,test.13"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0, "tag" => "test.12,test.13"})

      d.feed(time, {"metric" => "some.metric.name", "value" => 150.0, "tag" => "test.22,test.23"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 200.0, "tag" => "test.22,test.23"})

      d.feed(time, {"metric" => "some.metric.name", "value" => 250.0, "tag" => "test.32,test.33"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 300.0, "tag" => "test.32,test.33"})
    end
  end

  it 'should be called emit_points for each host' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {:host=>"www1.example.com"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 150.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 200.0]],
        {:host=>"www2.example.com"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 250.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 300.0]],
        {:host=>"www3.example.com"}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0, "host" => "www1.example.com"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0, "host" => "www1.example.com"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 150.0, "host" => "www2.example.com"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 200.0, "host" => "www2.example.com"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 250.0, "host" => "www3.example.com"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 300.0, "host" => "www3.example.com"})
    end
  end

  it 'should be called emit_points for each device' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {:device=>"device1"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 150.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 200.0]],
        {:device=>"device2"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 250.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 300.0]],
        {:device=>"device3"}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0, "device" => "device1"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0, "device" => "device1"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 150.0, "device" => "device2"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 200.0, "device" => "device2"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 250.0, "device" => "device3"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 300.0, "device" => "device3"})
    end
  end

  it 'should be called emit_points for each type' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {:type=>"gauge"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 150.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 200.0]],
        {:type=>"counter"}
      )

      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0, "type" => "gauge"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0, "type" => "gauge"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 150.0, "type" => "counter"})
      d.feed(time, {"metric" => "some.metric.name", "value" => 200.0, "type" => "counter"})
    end
  end

  it 'should be skipped if `metric` key does not exists' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.at(event_time("2014-02-08 04:14:15 UTC")), 50.0],
         [Time.at(event_time("2014-02-08 04:14:15 UTC")), 100.0]],
        {}
      )

      log = d.instance.log
      expect(log).to receive(:warn)
         .with('`metric` key does not exist: ["test.default", 1391832855, {"no metric"=>"some.metric.name", "value"=>51.0}]')
      expect(log).to receive(:warn)
         .with('`metric` key does not exist: ["test.default", 1391832855, {"no metric"=>"some.metric.name", "value"=>101.0}]')

      d.feed(time, {"no metric" => "some.metric.name", "value" => 51.0})
      d.feed(time, {"no metric" => "some.metric.name", "value" => 101.0})
      d.feed(time, {"metric" => "some.metric.name", "value" => 50.0})
      d.feed(time, {"metric" => "some.metric.name", "value" => 100.0})
    end
  end
end
