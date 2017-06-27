/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const { Router,
      Route,
      IndexRoute,
      IndexLink,
      hashHistory,
      Link } = ReactRouter;

class Chart extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            timeframe: '6h'
        };
    }

    intervalToMilliseconds(value) {
        const regexExtract = /(\d+)(.+)/g.exec(value);
        const val = regexExtract[1];
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

    render() {
        let title, notice;
        if (this.props.tenant && this.props.type && this.props.metric) {
            title = "Tenant '" + this.props.tenant + "', " + this.props.type + " '" + this.props.metric + "'";
        } else if (this.props.tenant) {
            title = "Tenant '" + this.props.tenant + "'";
            notice = "Select a metric from the left menu"
        } else {
            notice = "Enter a tenant then select a metric from the left menu"
        }
        return (
            <div className="chart">
                <h1>{title}</h1>
                <div className="spinner"></div>
                <h3>{notice}</h3>
                <div className="timeframe">
                    <select className="selectpicker"
                            value={this.state.timeframe}
                            onChange={this.onTimeFrameChange.bind(this)}>
                        <option value='5m'>Last 5 minutes</option>
                        <option value='15m'>Last 15 minutes</option>
                        <option value='30m'>Last 30 minutes</option>
                        <option value='1h'>Last hour</option>
                        <option value='6h'>Last 6 hours</option>
                        <option value='1d'>Last 24 hours</option>
                        <option value='2d'>Last 2 days</option>
                        <option value='7d'>Last week</option>
                        <option value='30d'>Last month</option>
                    </select>
                </div>
                <div id="line-chart" className="line-chart-pf"></div>
            </div>
        );
    }

    onTimeFrameChange(event) {
        const timeframe = event.target.value;
        this.setState({ timeframe: timeframe });
        const format = (timeframe[timeframe.length-1] == 'd') ? '%m-%d %H:%M:%S' : '%H:%M:%S';
        if (format != this.config.axis.x.tick.format) {
            // Need to regenerate chart with new axis
            this.config.axis.x.tick.format = format;
            this.chart = c3.generate(this.config);
        }
        this.fetchAndUpdate({ timeframe: timeframe });
    }

    componentDidMount() {
        $('.selectpicker').selectpicker();
        this.config = $().c3ChartDefaults().getDefaultSingleLineConfig();
        this.config.bindto = '#line-chart';
        this.config.data = {
            x: 'x',
            columns: [['x'],['metric']],
            type: 'line'
        };
        this.config.axis.x = {
            type: 'timeseries',
            tick: {
                format: '%H:%M:%S'
            }
        };
        delete this.config.tooltip;
        this.chart = c3.generate(this.config);
        this.fetchAndUpdate({ timeframe: this.state.timeframe });
    }

    componentDidUpdate() {
        this.fetchAndUpdate({ timeframe: this.state.timeframe });
    }

    fetchAndUpdate(options) {
        if (this.props.tenant && this.props.type && this.props.metric) {
            $(".chart .spinner").show();
            let typeForUrl = this.props.type;
            if (typeForUrl != "availability") {
                typeForUrl += "s";
            }
            const start = new Date().getTime() - this.intervalToMilliseconds(options.timeframe);
            // FIXME: manage non numeric types
            // Fetch datapoints
            $.ajax({
                url: "/hawkular/metrics/" + typeForUrl + "/" + encodeURI(this.props.metric)
                    + "/raw?order=ASC&start=" + start,
                contentType: "application/json",
                headers: {"Hawkular-Tenant": this.props.tenant},
                success: (datapoints, textStatus, xhr) => {
                    if (datapoints) {
                        this.chart.load({
                            columns: [
                                ['x'].concat(datapoints.map(dp => dp.timestamp)),
                                ['metric'].concat(datapoints.map(dp => dp.value))
                            ]
                        });
                    }
                },
                complete: () => {
                    $(".chart .spinner").hide();
                }
            });
        } else {
            this.chart.load({
                columns: [
                    ['x'],
                    ['metric']
                ]
            });
            $(".chart .spinner").hide();
        }
    }
}

class Metrics extends React.Component {
    constructor(props) {
        super(props);
        this.onTenantChanged = this.onTenantChanged.bind(this);
        this.state = {
            tenant: this.props.params.tenant || "",
            message: "",
            metrics: []
        };
        if (this.props.params && this.props.params.tenant) {
            this.state.metrics = ["Loading..."];
            this.fetchMetrics();
        }
    }

    render() {
        return (
            <div>
                <nav className="navbar navbar-sidebar">
                    <div className="section title">
                        Metrics
                        <div className="spinner spinner-inverse spinner-sm"></div>
                    </div>
                    <div className="section">
                        Tenant
                        <input type="text" value={this.state.tenant} onChange={this.onTenantChanged}/>
                    </div>
                    <div className="section">
                        {this.state.message}
                        <ul>
                            {this.state.metrics.map(metric => (
                                <li key={metric.id}>
                                    <a className="sidebar-item" role="button" title={metric.id} href={"#metrics/" + this.state.tenant + "/" + metric.type + "/" + metric.id}>
                                        {metric.id}
                                    </a>
                                </li>
                            ))}
                        </ul>
                    </div>
                </nav>
                <div className="container-fluid">
                    <Chart tenant={this.props.params.tenant} type={this.props.params.type} metric={this.props.params.metric} />
                </div>
            </div>
        )
    }

    onTenantChanged(event) {
        this.setState({tenant: event.target.value});
        if (this.tenantChangeHandle) {
            clearTimeout(this.tenantChangeHandle);
        }
        this.tenantChangeHandle = setTimeout(() => {
            this.fetchMetrics();
            this.tenantChangeHandle = null;
        }, 200);
    }

    componentDidMount() {
        $(".navbar-sidebar .spinner").hide();
    }

    fetchMetrics() {
        $(".navbar-sidebar .spinner").show();
        $.ajax({
            url: "/hawkular/metrics/metrics",
            contentType: "application/json",
            headers: {"Hawkular-Tenant": this.state.tenant},
            success: (metrics, textStatus, xhr) => {
                if (metrics && metrics.length > 0) {
                    this.setState({metrics: metrics.sort((a,b) => {
                        return a.id < b.id ? -1 : 1;
                    }), message: ""});
                } else {
                    this.setState({
                        metrics: [],
                        message: "No metric found for this tenant"
                    });
                }
            },
            complete: (xhr, textStatus) => {
                $(".navbar-sidebar .spinner").hide();
                if (xhr.status === 404 || xhr.status === 503) {
                    this.setState({
                        metrics: [],
                        message: "The server is not available"
                    });
                } else if (xhr.status >= 400) {
                    this.setState({
                        metrics: [],
                        message: "An error occured while accessing the server: " + textStatus
                    });
                }
            }
        });
    }
}

class Status extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            status: "Status: checking..."
        };
    }

    render() {
        return (
            <div>
                <nav className="navbar navbar-sidebar">
                    <div className="section title">
                        Status
                        <div className="spinner spinner-inverse spinner-sm"></div>
                    </div>
                </nav>
                <div className="container-fluid">
                    <div className="logo-big">
                        <img src="/hawkular/metrics/static/img/hawkular_logo.png" alt="Hawkular Logo"/>
                    </div>
                    <h1>Hawkular Metrics</h1>
                    <h3>A time series metrics engine based on Cassandra</h3>

                    <p>{this.state.version}</p>
                    <p>{this.state.gitref}</p>
                    <p>{this.state.status}</p>
                </div>
            </div>
        )
    }

    componentDidMount() {
        $(".navbar-sidebar .spinner").show();
        $.ajax({
            url: "/hawkular/metrics/status",
            success: (statusJson, textStatus, xhr) => {
                this.setState({
                    version: statusJson["Implementation-Version"],
                    gitref: "(Git SHA1 - " + statusJson["Built-From-Git-SHA1"] + ")",
                    status: "Metrics Service: " + statusJson.MetricsService
                });
            },
            complete: (xhr, textStatus) => {
                $(".navbar-sidebar .spinner").hide();
                if (xhr.status === 404 || xhr.status === 503) {
                    this.setState({
                        status: "The server is not available"
                    });
                } else if (xhr.status != 200) {
                    this.setState({
                        status: "An error occured while accessing the server: " + textStatus
                    });
                }
            }
        });
    }
}

const Menu = (
    <nav className="navbar navbar-default navbar-pf navbar-fixed-top" role="navigation">
        <div className="collapse navbar-collapse navbar-collapse-1">
            <div>
                <a href="http://www.hawkular.org/" className="navbar-brand">
                    <img className="navbar-brand-icon" src="/hawkular/metrics/static/img/hawkular_logo_small.png" alt=""/>
                </a>
                <a href="http://www.hawkular.org/" className="navbar-brand brand-name">Hawkular</a>
                <span className="navbar-brand brand-statement">A time series metrics engine based on Cassandra</span>
                <ul className="nav navbar-nav navbar-primary navbar-right">
                    <li><Link to="/metrics" activeClassName="active">Metrics</Link></li>
                    <li><Link to="/status" activeClassName="active">Status</Link></li>
                </ul>
            </div>
        </div>
    </nav>
)

const App = React.createClass({
    render: function() {
        return (
            <div>
                <div className="menu">{Menu}</div>
                <div className="content">
                    {this.props.children}
                </div>
            </div>
        )
    }
});

ReactDOM.render(
    <Router history={hashHistory}>
        <Route path="/" component={App}>
            <IndexRoute component={Metrics}/>
            <Route path="metrics/:tenant/:type/:metric" component={Metrics} />
            <Route path="metrics/:tenant" component={Metrics} />
            <Route path="metrics" component={Metrics} />
            <Route path="status" component={Status} />
        </Route>
    </Router>,
    document.getElementById('root')
);
