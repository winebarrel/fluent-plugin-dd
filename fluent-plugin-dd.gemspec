# coding: utf-8
Gem::Specification.new do |spec|
  spec.name          = 'fluent-plugin-dd'
  spec.version       = '0.1.3'
  spec.authors       = ['Genki Sugawara']
  spec.email         = ['sugawara@cookpad.com']
  spec.description   = %q{Output plugin for Datadog}
  spec.summary       = %q{Output plugin for Datadog}
  spec.homepage      = 'https://bitbucket.org/winebarrel/fluent-plugin-dd'
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_dependency 'fluentd'
  spec.add_dependency 'dogapi'
  spec.add_development_dependency 'bundler', '~> 1.3'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rspec', '>= 2.11.0'
end
