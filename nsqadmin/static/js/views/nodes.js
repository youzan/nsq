var $ = require('jquery');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var Nodes = require('../collections/nodes');

var NodesView = BaseView.extend({
    className: 'nodes container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .conn-count': 'onClickConnCount'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.collection = new Nodes();
        this.collection.fetch()
            .done(function(data) {
                var clusterInfo;
                this.template = require('./nodes.hbs');
                $.ajax({
                    url: AppState.url('/cluster/stats'),
                    success: function(data){
                        clustersInfo = data;
                    },
                    async: false
                });
                this.render({
                    'message': data['message'],
                    'clusters': clustersInfo
                });
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },

    onClickConnCount: function(e) {
        e.preventDefault();
        $(e.target).next().toggle();
    }
});

module.exports = NodesView;
