import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';
import { MetricChartComponent } from 'hawkular-charts';

@Component({
  selector: 'chart',
  templateUrl: './chart.component.html',
  styleUrls: ['./chart.component.css']
})
export class ChartComponent implements OnInit {
  timeframe: string = '6h';
  title: string = '';
  notice: string = '';
  tenant: string;
  type: string;
  metric: string;
  loading = false;
  constructor(private route: ActivatedRoute) {
  }

  ngOnInit(): void {
    this.route.params
      .subscribe((params: Params) => {
        this.tenant = params['tenant'];
        this.type = params['type'];
        this.metric = params['metric'];
        if (this.tenant && this.type && this.metric) {
            this.title = "Tenant '" + this.tenant + "', " + this.type + " '" + this.metric + "'";
        } else {
            this.notice = 'Enter a tenant then select a metric from the left menu'
        }
      });
  }

  intervalToMilliseconds(value) {
      const regexExtract = /(\d+)(.+)/g.exec(value);
      const val = +regexExtract[1];
      const unit = regexExtract[2];
      let mult = 1000;
      if (unit == 'm') {
          mult *= 60;
      } else if (unit == 'h') {
          mult *= 3600;
      } else if (unit == 'd') {
          mult *= 86400;
      }
      return val * mult;
  }

  onTimeFrameChanged(event) {
  }
}
