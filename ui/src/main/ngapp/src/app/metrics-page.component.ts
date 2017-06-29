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
    this.http.get('http://localhost:8080/hawkular/metrics/metrics', options)
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
