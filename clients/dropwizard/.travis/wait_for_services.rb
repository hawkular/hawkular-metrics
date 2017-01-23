#!/usr/bin/env ruby
#
# Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
# and other contributors as indicated by the @author tags.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'net/http'

uris = [
  'http://localhost:8080/hawkular/status',
  'http://localhost:8080/hawkular/metrics/status',
  'http://localhost:8080/hawkular/alerts/status',
  'http://localhost:8080/hawkular/inventory/status'
]

uris.each do |uri_string|
  loop do
    uri = URI(uri_string)
    begin
      response = Net::HTTP.get_response(uri)
      break if response.code == '200'
      puts "Waiting for: #{uri_string}"
    rescue
      puts 'Waiting for Hawkular-Services to accept connections'
    end
    sleep 5
  end
end
