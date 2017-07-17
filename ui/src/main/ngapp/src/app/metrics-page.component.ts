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

import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';
import { Http, RequestOptions, Headers, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/finally';

import { ChartComponent } from './chart.component';
import { Metric } from './model/metric';

@Component({
  selector: 'metrics-page',
  templateUrl: './metrics-page.component.html',
  styleUrls: ['./metrics-page.component.css']
})
export class MetricsPageComponent implements OnInit {
  message = '';
  tenant = '';
  metrics: Metric[] = [];
  hTypingTimeout = null;
  loading = false;

  constructor (private http: Http, private route: ActivatedRoute) {
  }

  ngOnInit(): void {
    this.route.firstChild.params
      .subscribe((params: Params) => {
        if (params['tenant'] && params['tenant'] != this.tenant) {
          this.tenant = params['tenant'];
          this.fetchMetrics();
        }
      });
  }

  onTenantChanged(event) {
    if (this.hTypingTimeout) {
        clearTimeout(this.hTypingTimeout);
    }
    this.hTypingTimeout = setTimeout(() => {
        this.fetchMetrics();
        this.hTypingTimeout = null;
    }, 200);
  }

  fetchMetrics() {
    this.loading = true;
    const options = new RequestOptions({ headers: new Headers({
      'Hawkular-Tenant': this.tenant
    })});
    this.http.get('/hawkular/metrics/metrics', options)
      .map((response) => response.json())
      .finally(() => this.loading = false)
      .subscribe((metrics: Metric[]) => {
        if (metrics && metrics.length > 0) {
          this.metrics = metrics.sort((a,b) => {
              return a.id < b.id ? -1 : 1;
          });
          this.message = '';
        } else {
          this.metrics = [];
          this.message = 'No metric found for this tenant';
        }
      }, (err) => {
        this.metrics = [];
        if (err.status == 0 && err.statusText == '') {
          this.message = 'Could not connect to the server';
        } else {
          this.message = 'An error occured while accessing the server: [' + err.status + '] ' + err.statusText;
        }
      });
  }
}
