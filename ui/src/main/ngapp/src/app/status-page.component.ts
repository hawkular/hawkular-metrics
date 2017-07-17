///
/// Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
/// and other contributors as indicated by the @author tags.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///    http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { Component } from '@angular/core';
import { ServerInfo } from './model/server-info';

import { Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/finally';

@Component({
  selector: 'status-page',
  templateUrl: './status-page.component.html',
  styleUrls: ['./status-page.component.css']
})
export class StatusPageComponent {
  serverInfo: ServerInfo = {
    status: 'Status: checking...',
    gitref: null,
    version: null
  };
  loading = true;

  constructor (http: Http) {
    http.get('/hawkular/metrics/status')
      .map((response) => response.json())
      .finally(() => this.loading = false)
      .subscribe((json) => {
        this.serverInfo = {
          status: 'Metrics Service: ' + json['MetricsService'],
          gitref: '(Git SHA1 - ' + json['Built-From-Git-SHA1'] + ')',
          version: json['Implementation-Version']
        };
      }, (err) => {
        if (err.status == 0 && err.statusText == '') {
          this.serverInfo = {
            status: 'Could not connect to the server',
            gitref: null,
            version: null
          };
        } else {
          this.serverInfo = {
            status: 'An error occured while accessing the server: [' + err.status + '] ' + err.statusText,
            gitref: null,
            version: null
          };
        }
      });
  }
}
