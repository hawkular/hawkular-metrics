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
    http.get('http://localhost:8080/hawkular/metrics/status')
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
