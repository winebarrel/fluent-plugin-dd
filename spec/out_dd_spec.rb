describe Fluent::DdOutput do
  let(:time) {
    Time.parse('2014-02-08 04:14:15 UTC').to_i
  }

  it 'should load config' do
    driver = Fluent::Test::OutputTestDriver.new(Fluent::DdOutput, 'test.default')

    driver.configure(<<-EOS)
      type dd
      dd_api_key API_KEY
      dd_app_key APP_KEY
      host my_host.example.com
      use_fluentd_tag_for_datadog_tag true
      emit_in_background true
    EOS

    expect(driver.instance.dd_api_key).to eq 'API_KEY'
    expect(driver.instance.dd_app_key).to eq 'APP_KEY'
    expect(driver.instance.host).to eq 'my_host.example.com'
    expect(driver.instance.use_fluentd_tag_for_datadog_tag).to eq true
    expect(driver.instance.emit_in_background).to eq true
  end

  it 'should receive an API key' do
    expect(Dogapi::Client).to receive(:new).with("test_dd_api_key", nil, "test_host")
    run_driver {|d, dog| }
  end

  it 'should be called emit_points' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {}
      )

      d.emit({"metric" => "some.metric.name", "value" => 50.0}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0}, time)
    end
  end

  context 'with emit_in_background' do
    before do
      $threads_array_for_test = []
    end

    after do
      $threads_array_for_test = nil
    end

    it 'should be called emit_points in the background' do
      run_driver(:emit_in_background => true) do |d, dog|
        expect(dog).to receive(:emit_points).with(
          "some.metric.name",
          [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
           [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
          {}
        )

        d.emit({"metric" => "some.metric.name", "value" => 50.0}, time)
        d.emit({"metric" => "some.metric.name", "value" => 100.0}, time)
      end
    end

    it 'should be called emit_points in the background and in the different thread' do
      run_driver(
        :emit_in_background => true,
        :concurrency => 2,
      ) do |d, dog|
        expect(dog).to receive(:emit_points).with(
          "some.metric.name.1",
          [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0]],
          {}
        )
        expect(dog).to receive(:emit_points).with(
          "some.metric.name.2",
          [[Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
          {}
        )

        d.emit({"metric" => "some.metric.name.1", "value" => 50.0}, time)
        d.emit({"metric" => "some.metric.name.2", "value" => 100.0}, time)
      end

      expect($threads_array_for_test.size).to eq(2)
      expect($threads_array_for_test[0]).not_to eq($threads_array_for_test[1])
    end
  end

  it 'should be called emit_points with tag' do
    run_driver(:use_fluentd_tag_for_datadog_tag => true) do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {:tags=>["test.default"]}
      )

      d.emit({"metric" => "some.metric.name", "value" => 50.0}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0}, time)
    end
  end

  it 'should be called emit_points for each tag' do
    run_driver(:use_fluentd_tag_for_datadog_tag => true) do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {:tags=>["test.1"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 150.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 200.0]],
        {:tags=>["test.2"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 250.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 300.0]],
        {:tags=>["test.3"]}
      )

      d.tag = 'test.1'
      d.emit({"metric" => "some.metric.name", "value" => 50.0}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0}, time)

      d.tag = 'test.2'
      d.emit({"metric" => "some.metric.name", "value" => 150.0}, time)
      d.emit({"metric" => "some.metric.name", "value" => 200.0}, time)

      d.tag = 'test.3'
      d.emit({"metric" => "some.metric.name", "value" => 250.0}, time)
      d.emit({"metric" => "some.metric.name", "value" => 300.0}, time)
    end
  end

  it 'should be called emit_points for each tag (tag is included in the record)' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {:tags=>["test.11"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 150.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 200.0]],
        {:tags=>["test.21"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 250.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 300.0]],
        {:tags=>["test.31"]}
      )

      d.emit({"metric" => "some.metric.name", "value" => 50.0,  "tag" => "test.11"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0, "tag" => "test.11"}, time)

      d.emit({"metric" => "some.metric.name", "value" => 150.0, "tag" => "test.21"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 200.0, "tag" => "test.21"}, time)

      d.emit({"metric" => "some.metric.name", "value" => 250.0, "tag" => "test.31"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 300.0, "tag" => "test.31"}, time)
    end
  end

  it 'should be called emit_points with multiple tags' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {:tags=>["test.12","test.13"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 150.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 200.0]],
        {:tags=>["test.22","test.23"]}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 250.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 300.0]],
        {:tags=>["test.32","test.33"]}
      )

      d.emit({"metric" => "some.metric.name", "value" => 50.0,  "tag" => "test.12,test.13"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0, "tag" => "test.12,test.13"}, time)

      d.emit({"metric" => "some.metric.name", "value" => 150.0, "tag" => "test.22,test.23"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 200.0, "tag" => "test.22,test.23"}, time)

      d.emit({"metric" => "some.metric.name", "value" => 250.0, "tag" => "test.32,test.33"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 300.0, "tag" => "test.32,test.33"}, time)
    end
  end

  it 'should be called emit_points for each host' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {:host=>"www1.example.com"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 150.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 200.0]],
        {:host=>"www2.example.com"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 250.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 300.0]],
        {:host=>"www3.example.com"}
      )

      d.emit({"metric" => "some.metric.name", "value" => 50.0, "host" => "www1.example.com"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0, "host" => "www1.example.com"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 150.0, "host" => "www2.example.com"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 200.0, "host" => "www2.example.com"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 250.0, "host" => "www3.example.com"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 300.0, "host" => "www3.example.com"}, time)
    end
  end

  it 'should be called emit_points for each type' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {:type=>"gauge"}
      )

      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 150.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 200.0]],
        {:type=>"counter"}
      )

      d.emit({"metric" => "some.metric.name", "value" => 50.0, "type" => "gauge"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0, "type" => "gauge"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 150.0, "type" => "counter"}, time)
      d.emit({"metric" => "some.metric.name", "value" => 200.0, "type" => "counter"}, time)
    end
  end

  it 'should be skipped if `metric` key does not exists' do
    run_driver do |d, dog|
      expect(dog).to receive(:emit_points).with(
        "some.metric.name",
        [[Time.parse("2014-02-08 04:14:15 UTC"), 50.0],
         [Time.parse("2014-02-08 04:14:15 UTC"), 100.0]],
        {}
      )

      log = d.instance.log
      expect(log).to receive(:warn)
         .with('`metric` key does not exist: ["test.default", 1391832855, {"no metric"=>"some.metric.name", "value"=>51.0}]')
      expect(log).to receive(:warn)
         .with('`metric` key does not exist: ["test.default", 1391832855, {"no metric"=>"some.metric.name", "value"=>101.0}]')

      d.emit({"no metric" => "some.metric.name", "value" => 51.0}, time)
      d.emit({"no metric" => "some.metric.name", "value" => 101.0}, time)
      d.emit({"metric" => "some.metric.name", "value" => 50.0}, time)
      d.emit({"metric" => "some.metric.name", "value" => 100.0}, time)
    end
  end
end
