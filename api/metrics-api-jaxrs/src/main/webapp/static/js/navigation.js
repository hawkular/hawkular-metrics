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
        console.log("ctor Chart");
    }

    render() {
        console.log("render Chart");
        let title, notice;
        if (this.props.tenant && this.props.type && this.props.metric) {
            title = "Tenant '" + this.props.tenant + "', " + this.props.type + " '" + this.props.metric + "'";
        } else if (this.props.tenant) {
            title = "Tenant '" + this.props.tenant + "'";
            notice = "Select a metric from the left menu"
        } else {
            notice = "Select a tenant and a metric from the left menu"
        }
        return (
            <div>
                <h1>{title}</h1><h3>{notice}</h3>
                <div id="line-chart" class="line-chart-pf"></div>
            </div>
        );
    }

    componentDidMount() {
        console.log("did mount Chart");
        const config = $().c3ChartDefaults().getDefaultSingleLineConfig();
        config.bindto = '#line-chart';
        config.data = {
            x: 'x',
            columns: [['x'],['data1']],
            type: 'line'
        };
        config.axis.x = {
            type: 'timeseries',
            tick: {
                format: '%I:%M:%S'
            }
        };
        const chart = c3.generate(config);
        if (this.props.tenant && this.props.type && this.props.metric) {
            let typeForUrl = this.props.type;
            if (typeForUrl != "availability") {
                typeForUrl += "s";
            }
            // FIXME: manage non numeric types
            // Fetch datapoints
            $.ajax({
                url: "/hawkular/metrics/" + typeForUrl + "/" + encodeURI(this.props.metric) + "/raw?order=ASC",
                contentType: "application/json",
                headers: {"Hawkular-Tenant": this.props.tenant},
                success: (datapoints, textStatus, xhr) => {
                    if (datapoints) {
                        chart.load({
                            columns: [
                                ['x'].concat(datapoints.map(dp => dp.timestamp)),
                                ['data1'].concat(datapoints.map(dp => dp.value))
                            ]
                        });
                    }
                }
            });
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
        console.log("Rendering Metrics");
        return (
            <div>
                <nav className="navbar navbar-sidebar">
                    <span className="section title">Metrics</span>
                    <div className="section">
                        Tenant
                        <input type="text" value={this.state.tenant} onChange={this.onTenantChanged}/>
                    </div>
                    <div className="section">
                        {this.state.message}
                        <ul>
                            {this.state.metrics.map(metric => (
                                <li key={metric.id}>
                                    <a className="sidebar-item" role="button" href={"#metrics/" + this.state.tenant + "/" + metric.type + "/" + metric.id}>
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

    fetchMetrics() {
        $.ajax({
            url: "/hawkular/metrics/metrics",
            contentType: "application/json",
            headers: {"Hawkular-Tenant": this.state.tenant},
            success: (metrics, textStatus, xhr) => {
                if (metrics.length > 0) {
                    this.setState({metrics: metrics.sort((a,b) => {
                        return a.id < b.id ? -1 : 1;
                    }), message: ""});
                } else {
                    this.setState({
                        message: "No metric found for this tenant"
                    });
                }
            },
            complete: (xhr, textStatus) => {
                if (xhr.status === 404 || xhr.status === 503) {
                    this.setState({
                        message: "The server is not available"
                    });
                } else if (xhr.status != 200) {
                    this.setState({
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
            "Status": "Status: checking...",
            "Implementation-Version": "",
            "Built-From-Git-SHA1": ""
        };
    }

    render() {
        return (
            <div>
                <nav className="navbar navbar-sidebar">
                    <span className="title">Status</span>
                    <ul>
                        <li>Services</li>
                        <li>Config</li>
                    </ul>
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
        $.ajax({
            url: "/hawkular/metrics/status",
            success: (data, textStatus, xhr) => {
                const statusJson = JSON.parse(textStatus);
                this.setState({
                    version: statusJson["Implementation-Version"],
                    gitref: "(Git SHA1 - " + statusJson["Built-From-Git-SHA1"] + ")",
                    status: "Metrics Service: " + statusJson.MetricsService
                });
            },
            complete: (xhr, textStatus) => {
                if (xhr.status === 404 || xhr.status === 503) {
                    this.setState({
                        status: "The server is not available"
                    });
                } else if (xhr != 200) {
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
