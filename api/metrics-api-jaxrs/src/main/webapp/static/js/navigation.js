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

const Metrics = () => (
    <div>
        <h2>Metrics</h2>
    </div>
)

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
                <div className="logo-big">
                    <img src="/hawkular/metrics/static/img/hawkular_logo.png" alt="Hawkular Logo"/>
                </div>
                <h1>Hawkular Metrics</h1>
                <h3>A time series metrics engine based on Cassandra</h3>

                <p>{this.state.version}</p>
                <p>{this.state.gitref}</p>
                <p>{this.state.status}</p>
            </div>
        )
    }

    componentDidMount() {
        var httpRequest;

        if (window.XMLHttpRequest) {
            httpRequest = new XMLHttpRequest();
        } else if (window.ActiveXObject) {
            try {
                httpRequest = new ActiveXObject("Msxml2.XMLHTTP");
            } catch (e) {
                httpRequest = new ActiveXObject("Microsoft.XMLHTTP");
            }
        }

        httpRequest.onreadystatechange = () => {
            if (httpRequest.readyState === 4) {
                if (httpRequest.status === 200) {
                    const statusJson = JSON.parse(httpRequest.responseText);
                    this.setState({
                        version: statusJson["Implementation-Version"],
                        gitref: "(Git SHA1 - " + statusJson["Built-From-Git-SHA1"] + ")",
                        status: "Metrics Service: " + statusJson.MetricsService
                    });
                } else if (httpRequest.status === 404 || httpRequest.status === 503) {
                    this.setState({
                        status: "The server is not available"
                    });
                } else {
                    this.setState({
                        status: "An error occured while accessing the server :" + httpRequest.responseText
                    });
                }
            }
        };
        httpRequest.open("GET", "/hawkular/metrics/status");
        httpRequest.send();
    }
}

const Menu = (
    <nav className="navbar navbar-default navbar-pf" role="navigation">
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
            <Route path="metrics" component={Metrics} />
            <Route path="status" component={Status} />
        </Route>
    </Router>,
    document.getElementById('root')
);
