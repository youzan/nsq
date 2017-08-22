var _ = require('underscore');
var Backbone = require('backbone');

var AppState = require('../app_state');

var Topics = require('./topics');

var TopicsMeta = Topics.extend({

    url: function() {
        return AppState.url('/topics?metaInfo=true');
    }
});

module.exports = TopicsMeta;
