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

import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { HttpModule } from '@angular/http';

import { HawkularChartsModule } from 'hawkular-charts';

import { AppComponent } from './app.component';
import { MetricsPageComponent } from './metrics-page.component';
import { StatusPageComponent } from './status-page.component';
import { ChartComponent } from './chart.component';

@NgModule({
  declarations: [
    AppComponent,
    MetricsPageComponent,
    StatusPageComponent,
    ChartComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule,
    HawkularChartsModule,
    RouterModule.forRoot([{
        path: '',
        redirectTo: '/metrics',
        pathMatch: 'full'
      }, {
        path: 'metrics',
        component: MetricsPageComponent,
        children: [{
          path: ':tenant/:type/:metric',
          component: ChartComponent,
          outlet: 'chart'
        }, {
          path: ':tenant',
          component: ChartComponent,
          outlet: 'chart'
        }, {
          path: '',
          component: ChartComponent,
          outlet: 'chart'
        }]
      }, {
        path: 'status',
        component: StatusPageComponent
      }
    ])
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
