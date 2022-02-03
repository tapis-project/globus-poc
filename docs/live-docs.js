// $(document).foundation();
// Default branch is local
let urlParms = new URLSearchParams(location.search);
let branch_name = urlParms.has('branch') ? urlParms.get('branch') : 'local';
// list of APIS
var apis = [
    {
        name: 'GlobusProxy',
        url: 'https://raw.githubusercontent.com/tapis-project/openapi-globus-proxy/' + branch_name + '/GlobusProxyAPI.yaml'
    }
];

function init() {

    let service = urlParms.get("service");
    if (service) {
        apis.forEach((d) => {
            if (d.name.toLowerCase() == service.toLowerCase()) {
                Redoc.init(d.url);
            }
        });
    } else {
        // initially render first API
        Redoc.init(apis[0].url);
    }
}
$(document).ready(function($) {

    function onClick() {
        var url = this.getAttribute('data-link');
        let serviceName = this.getAttribute('service');
        Redoc.init(url);
        var queryParams = new URLSearchParams(window.location.search);
        queryParams.set("service", serviceName);
        // history.replaceState(null, null, "?"+queryParams.toString());
        // history.pushState(null, null, "?"+queryParams.toString());
        window.location.search = queryParams.toString();

    }

    // dynamically building navigation items
    var $list = document.getElementById('links_container');
    apis.forEach(function (api) {
        var $listitem = document.createElement('li');
        $listitem.setAttribute('data-link', api.url);
        $listitem.setAttribute('service', api.name);
        $listitem.innerText = api.name;
        $listitem.addEventListener('click', onClick);
        $list.appendChild($listitem);
    });

    init();

    $(window).on('popstate', function() {
        init();
    });
})
