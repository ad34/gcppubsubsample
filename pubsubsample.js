const { PubSub } = require('@google-cloud/pubsub');
const pino = require('pino')();

const crypto = require('crypto');

const subId = crypto.createHash('md5').update(new Date().toString()).digest('hex');
const subName = `sub-${subId}`;

const cwd = process.cwd();
const keyFilename = `${cwd}/gcp.json`;

const projectId = 'myprojectId';
const topicName = 'jobs';

const config = {
    projectId,
    keyFilename,
};

const createSubscription = async (topicName, subscriptionName) => {
    const pubsub = new PubSub(config);
    await pubsub
        .topic(topicName)
        .createSubscription(subscriptionName)
        .catch((err) => {
            pino.error({ err: err.message }, 'pubsub error while creating subscription');
        });
    pino.info(`Subscription ${subscriptionName} created.`);
};

const subscribe = (subscriptionName, handler) => {
    const pubsub = new PubSub(config);
    const subscription = pubsub.subscription(subscriptionName);
    const messageHandler = (message) => {
        let data = null;
        pino.info({ id: message.id }, 'msg received');
        message.ack();
        if (handler) {
            handler({
                ...message,
                data,
            });
        }
    };

    // Listen for new messages until timeout is hit
    subscription.on('message', messageHandler);
    return subscription;
};

const publish = async (topicName, data) => {
    const stringified = JSON.stringify(data);
    const dataBuffer = Buffer.from(stringified);
    const pubsub = new PubSub(config);
    const topic = pubsub.topic(topicName);
    const messageId = await topic.publish(dataBuffer).catch((err) => {
        pino.error({ err: err.message }, 'pubsub publish error');
    });
    pino.info({ messageId }, 'pubsub msg published');
    return messageId;
}

createSubscription(topicName, subName).finally(() => {

    subscribe(subName, (msg) => {
        pino.info({ id: msg.id }, 'pubsub message');
        process.exit();
    });

    setTimeout(() => {

        const msgId = publish(topicName, { job: { jobNodeId: 'test' }, msg: 'gcp sample' });
        pino.info({ msgId }, 'msg published');
    }, 2000);

});
