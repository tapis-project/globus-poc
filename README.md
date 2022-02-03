# openapi-globusproxy

OpenAPI Specification for Tapis Globus Proxy Service

Contains this README file and the openapi specification file
GlobusProxyAPI.yaml.

There are four branches: local, dev, staging and prod.

All changes should first be made in the branch *local*. Work
in progress can be pushed to local without impacting live-docs.

Updates can then be moved from *local* to *dev* when the code
in *tapis-client-java* and the Tapis service code is updated to match
the openapi spec and deployed to the DEV kubernetes environment.

Moving from *dev* to *staging* and finally *staging* to *prod*
should proceed in the same manner.

The links in the live-docs repostiory index file located at
```
https://github.com/tapis-project/live-docs/blob/master/index.html
```
should point to the GlobusProxyAPI.yaml file located here.

