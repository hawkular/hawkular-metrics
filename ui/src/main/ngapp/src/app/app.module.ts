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
