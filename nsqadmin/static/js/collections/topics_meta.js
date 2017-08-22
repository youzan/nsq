var _ = require('underscore');
var Backbone = require('backbone');

var AppState = require('../app_state');

var Topics = require('./topics');
var Topic = require('../models/topic');

var TopicsMeta = Backbone.Collection.extend({
    model: Topic,

    comparator: 'id',

    constructor: function TopicsMeta() {
        Backbone.Collection.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.url('/topics?metaInfo=true');
    },

});

module.exports = TopicsMeta;