# Configuration

Since version 0.8.0, most configurable components of Colossus can be configured
using [Typesafe Config](https://github.com/typesafehub/config).

In general, all configuration is path-based, meaning that the supplied name of
the server/client/etc. is used in resolving the path for configuration.  When
path-based configuration is not present, each component falls back to a set of
defaults defined in Colossus'
[reference.conf](https://github.com/tumblr/colossus/blob/master/colossus/src/main/resources/reference.conf).

All configurable components have a constructor that takes a typesafe `Config` an
optional parameter.  If omitted, a config object will be loaded using
`ConfigFactory.load()`.

### Configuring IOSystems

Colossus will look for configuration in `colossus.iosystem`.

If an `IOSystem` is constructed without a `MetricSystem`, it will create a new one with the same name.

### Configuring Servers

Servers will look for path-based configuration at the path
`colossus.server.<name>`, falling back to just `colossus.server` for defaults.
See the API documentation for @extref[Server](docs:colossus.core.Server$) and @extref[ServerSettings](docs:colossus.core.ServerSettings) for more details

### Configuring Service Request Handlers

Request handlers will look for path-based config in the path
`colossus.service.<name>`, where "name" is the name of the server of the request
handler.  See the API docs for @extref[Service](docs:colossus.service.Service) and @extref[ServiceConfig](docs:colossus.service.ServiceConfig) for more details.

### Configuring Service Clients

Clients will look for path-based config in the path `colossus.client.<name>` where name is the name of the client. See 
the API docs for @extref[ServiceClientFactory](docs:colossus.service.ClientFactory) and 
@extref[ClientConfig](docs:colossus.service.ClientConfig) for more details.







