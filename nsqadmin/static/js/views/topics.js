var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');
var Topics = require('../collections/topics');
var TopicsMeta = require('../collections/topics_meta');
var TopicsView = BaseView.extend({
    className: 'topics container-fluid',

    template: require('./topics.hbs'),

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.collection = new Topics();
        this.collection.fetch()
            .done(function(data) {
                this.render({'message': data['message']});
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    }
});

module.exports = TopicsView;