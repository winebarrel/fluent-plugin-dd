# fluent-plugin-dd

Output plugin for Datadog

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-dd.svg)](http://badge.fury.io/rb/fluent-plugin-dd)
[![Build Status](https://travis-ci.org/winebarrel/fluent-plugin-dd.svg?branch=master)](https://travis-ci.org/winebarrel/fluent-plugin-dd)

## Installation

    $ gem install fluent-plugin-dd

## Configuration

```
<match datadog.**>
  type dd
  dd_api_key ...
  #dd_app_key_key ...
  #host my_host.example.com
  #device my_device
  #silent true
  #timeout 5
  #use_fluentd_tag_for_datadog_tag false
  #emit_in_background false
</match>
```

## Usage

```sh
echo '{"metric":"some.metric.name", "value":50.0}' | fluent-cat datadog.metric
echo '{"metric":"some.metric.name", "value":100.0, "tag":"any.tag", "host":"any.host", "type":"gauge", "device":"my_device"}' | fluent-cat datadog.metric
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
