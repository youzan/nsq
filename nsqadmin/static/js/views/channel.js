var $ = require('jquery');

window.jQuery = $;
var bootstrap = require('bootstrap'); //eslint-disable-line no-unused-vars
var bootbox = require('bootbox');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');
var click2Show=" >>>";
var click2Hide=" <<<";
var ChannelView = BaseView.extend({
    className: 'channel container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .consumer-actions button': 'consumerAction',
        'click .channel-actions button': 'channelAction',
        'click button#finish-msg': 'finishMessageAction',
        'blur .channel-actions input#resetChannelDatetime': 'resettsValidate',
        'click .toggle h4': 'onToggle',
        'click .toggle h4 span a': 'onToggle',
    },

    onToggle: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var canHideClass = "canHide";
        var parent = $(e.target).parents(".toggle").first();
        var divCanHide = parent.next();
        if(divCanHide != null && divCanHide.attr("class") != null && divCanHide.attr("class").indexOf(canHideClass) !== -1) {
            divCanHide.toggle(300);
            var anchor = parent.find("a").first();
            if(anchor.text().indexOf(click2Hide) !== -1) {
                anchor.text(click2Show);
            } else {
                anchor.text(click2Hide);
            }
        }
    },

    resettsValidate: function(e) {
        if(!event.target.checkValidity() || event.target.value === '') {
            $('.channel-actions button.resetbtn').prop('disabled', true);
        } else {
            $('.channel-actions button.resetbtn').prop('disabled', false);
        }
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.model.fetch()
            .done(function(data) {
                this.template = require('./channel.hbs');
                this.render({'message': data['message']});
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },

    finishMessageAction: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var action = $(e.currentTarget).data('action');
        var node = $(e.currentTarget).data('node');
        var partition = $(e.currentTarget).data('partition');
        var topic = this.model.get('topic');
        var callback = function(msgId) {
                           if(msgId && msgId<=0) {
                               this.showError("Invalid message internal ID");
                           } else if(msgId) {
                               $.post(this.model.clientUrl(), JSON.stringify({'action': action, 'node': node, 'msgid': msgId, 'partition': partition}))
                                   .done(function() { window.location.reload(true); })
                                   .fail(this.handleAJAXError.bind(this));
                           }
                       }.bind(this);

        bootbox.prompt({
            title: "input message internal ID you would like to FIN",
            inputType: 'number',
            callback: callback
        });
    },

    channelAction: function(e) {
        e.preventDefault();
        e.stopPropagation();
        var action = $(e.currentTarget).data('action');
        var txt = 'Are you sure you want to <strong>' +
            action + '</strong> <em>' + this.model.get('topic') +
            '/' + this.model.get('name') + '</em>';
        var ts
        if(action === 'reset') {
            ts = parseInt($('#resetChannelDatetime:first').val());
            txt = txt + ' to <strong>' + ts + '</strong> in second';
        }
        txt = txt + '?';
        var topic = this.model.get('topic')
        bootbox.confirm(txt, function(result) {
            if (result !== true) {
                return;
            }
            if (action === 'delete') {
                $.ajax(this.model.url(), {'method': 'DELETE'})
                    .done(function() {
                        window.location = '/topics/' + encodeURIComponent(topic);
                    })
                    .fail(this.handleAJAXError.bind(this));
            } else if (action === 'reset') {
               //parse timestamp
               $.post(this.model.url(), JSON.stringify({'action': action, 'timestamp': '' + ts}))
                                   .done(function() { window.location.reload(true); })
                                   .fail(this.handleAJAXError.bind(this));
            } else {
                $.post(this.model.url(), JSON.stringify({'action': action}))
                    .done(function() { window.location.reload(true); })
                    .fail(this.handleAJAXError.bind(this));
            }
        }.bind(this));
    },

    consumerAction: function(e) {
            e.preventDefault();
            e.stopPropagation();
            var action = $(e.currentTarget).data('action');
            var txt = 'Are you sure you want to <strong>' +
                action + '</strong> <em>' + this.model.get('topic') +
                '/' + this.model.get('name') + '</em>';
            txt = txt + '?';
            var topic = this.model.get('topic')
            var order = this.model.get('is_multi_ordered')
            bootbox.confirm(txt, function(result) {
                if (result !== true) {
                    return;
                }

                $.post(this.model.url() + "/client", JSON.stringify({
                                                'action': action,
                                                'order': order
                                            }))
                    .done(function() { window.location.reload(true); })
                    .fail(this.handleAJAXError.bind(this));
            }.bind(this));
        }
});

module.exports = ChannelView;
