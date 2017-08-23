var _ = require('underscore');
var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');
var TopicsView = require('./topics');
var TopicsMeta = require('../collections/topics_meta');
var metaFetched = false;
var TopicsFilterView = BaseView.extend({
    className: 'topicsFilter container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .filters input': 'topicsFilterAction',
    },

    filterTopics: function(e) {
        $('.table tr:not(.title)').show();

        $(e.target).parent().find('input')
            .filter(function(){
                if($(this).is(':checked')) {
                    return this;
                }
            })
            .each(function() {
                var filterValue = $(this).val();
                $('.table tr:not(.title)')
                    .filter(
                        function(){
                            if ($(this)
                                .find('td a#' + filterValue).length == 0){
                                return this;
                            }
                        }
                    )
                    .hide();
                }
            );
    },

    topicsFilterAction: function(e) {
            e.stopPropagation();
            if(!metaFetched) {
                var topicsView = this.topicsView;
                var filter = this.filterTopics;
                var topicsMeta = new TopicsMeta();
                topicsMeta.fetch()
                    .done(function(data){
                        var topics = _.map(data['topics'], function(topic) {
                                    return {
                                        'name':             topic['topic_name'],
                                        'extend_support':   topic['extend_support'],
                                        'ordered':          topic['ordered']
                                    };
                            });

                        topicsView.render(
                        {
                            'message': data['message'],
                            'collection': topics
                        });
                        metaFetched = true;
                        console.log("topic meta fetched.")
                        filter(e);
                    });
            } else {
                this.filterTopics(e);
            }

    },

    initialize: function(options){
        metaFetched = false;
        this.template = require('./topicsFilter.hbs');
        this.topicsView = new TopicsView();
    },

    render: function() {
            BaseView.prototype.initialize.apply(this, arguments);
            this.$el.html(this.template());
            this.$el.prepend(this.topicsView.render().el);
            return this;
    }
});

module.exports = TopicsFilterView;