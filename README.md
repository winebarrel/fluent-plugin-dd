# fluent-plugin-dd

Output plugin for Datadog

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-dd.png)](http://badge.fury.io/rb/fluent-plugin-dd)
[![Build Status](https://drone.io/bitbucket.org/winebarrel/fluent-plugin-dd/status.png)](https://drone.io/bitbucket.org/winebarrel/fluent-plugin-dd/latest)

## Installation

    $ gem install fluent-plugin-dd

## Configuration

```
<match datadog.**>
  type dd
  dd_api_key ...
  #host my_host.example.com
</match>
```

## Usage

```sh
echo '{"metric":"some.metric.name", "value":50.0}' | fluent-cat datadog.metric
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
