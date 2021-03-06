'use strict';

const Hoek = require('@hapi/hoek');

const Schmervice = require('schmervice');

const KINESIS_API_VERSION = '2013-12-02';

const internals = {};

module.exports = class KinesisService extends Schmervice.Service {

    constructor(server, options) {

        super(server, options);

        const { awsService } = server.services();

        const { isDstObserved } = internals;

        const { streamName, kinesisStreamName } = awsService.getServiceOptions('kinesis');

        this.streamName = streamName || kinesisStreamName;

        // No streamName, no service
        if (!this.streamName) {
            return;
        }

        this.isDstObserved = isDstObserved();

        this.kinesis = awsService.getSDKServiceInstance({
            sdkName: 'Kinesis',
            pluginOptionsName: 'kinesis',
            apiVersion: KINESIS_API_VERSION
        });
    }

    async putRecord(record) {

        this._ensureConfiguration();

        return await this.kinesis.putRecord({
            ...this.formatRecord(record),
            StreamName: this.streamName
        }).promise();
    }

    async putRecords(records) {

        this._ensureConfiguration();

        records = [].concat(records);

        Hoek.assert(records.length <= 500, 'There\'s an AWS limit of 500 per kinesis putRecords request. Page them.');

        return await this.kinesis.putRecords({
            Records: records.map(this.formatRecord.bind(this)),
            StreamName: this.streamName
        }).promise();
    }

    async putTestRecords(numRecords = 10, { usePutRecord = false } = {}) {

        this._ensureConfiguration();

        const { randomSleep } = internals;

        if (usePutRecord) {
            const results = [];

            for (let i = 0; i < numRecords; ++i) {
                const testRecord = this.genTestEvent();
                results.push(await this.putRecord(this.genTestEvent()));
                this.server.log(['kinesis', 'test'], `Sent ${testRecord}`);
                await randomSleep();
            }

            return results;
        }

        const testEvents = [];

        for (let i = 0; i < numRecords; ++i) {
            testEvents.push(this.genTestEvent());
        }

        return await this.putRecords(testEvents);
    }

    formatRecord(record) {

        const { localeStringTimezone, localeStringAbbrev } = this.options;
        const { genId } = internals;
        const now = new Date();

        return {
            Data: Buffer.from(JSON.stringify({
                ...record,
                tags: record.tags ? internals.arrayIfy(record.tags) : [],
                timestamp: record.timestamp || now.getTime(),
                timestampReadable: record.timestampReadable || `${now.toLocaleString('en-US', { timeZone: localeStringTimezone || 'America/New_York' })} ${localeStringAbbrev || (localeStringTimezone ? '' : (this.isDstObserved ? 'EDT' : 'EST'))}`
            })),
            PartitionKey: genId()
        };
    }

    genTestEvent() {

        const { localeStringTimezone, localeStringAbbrev } = this.options;
        const { pickRandom } = internals;

        const events = [
            'click',
            'pageview',
            'conversion',
            'error',
            'playvideo',
            'login',
            'logoff'
        ];

        const metrics = [
            'pollResponse',
            'displayXY',
            'blockerType',
            'browserVersion',
            'purchaseAmount',
            'gpuDriverVersion',
            'pagePercentDisplayed',
            'videoStoppedLocation',
            'computeRenderTime',
            'pageLoadTime',
            'cartItemQuantity',
            'impressionCount',
            'idleTimeMs',
            'mouseDistancePixels'
        ];

        const now = new Date();

        return {
            source: 'awschome/kinesisService',
            tags: [pickRandom(pickRandom([events, metrics]))],
            value: String(Math.random()).split('').pop(),
            isTest: true,
            timestamp: now.getTime(),
            timestampReadable: `${now.toLocaleString('en-US', { timeZone: localeStringTimezone || 'America/New_York' })} ${localeStringAbbrev || (localeStringTimezone ? '' : (this.isDstObserved ? 'EDT' : 'EST'))}`
        };
    }

    _ensureConfiguration() {

        Hoek.assert(this.kinesis && this.streamName, 'kinesisService is improperly configured');
    }
};

internals.genId = () => String(Date.now() * Math.random()).replace('.', '').slice(0, 15);

internals.pickRandom = (arr) => arr[Math.floor(Math.random() * arr.length)];

internals.randomSleep = async (min = 1, max = 5) => await new Promise((res) => setTimeout(res, internals.randomNumber(min, max)));

internals.randomNumber = (min, max) => Math.floor(Math.random() * (max - min) + min);

internals.arrayIfy = (val) => [].concat(val);

// Grabbed this isDstObserved stuff from https://stackoverflow.com/questions/11887934/how-to-check-if-the-dst-daylight-saving-time-is-in-effect-and-if-it-is-whats#answer-11888430
internals.stdTimezoneOffset = () => {

    const d = new Date();
    const jan = new Date(d.getFullYear(), 0, 1);
    const jul = new Date(d.getFullYear(), 6, 1);
    return Math.max(jan.getTimezoneOffset(), jul.getTimezoneOffset());
};

// Grabbed this isDstObserved stuff from https://stackoverflow.com/questions/11887934/how-to-check-if-the-dst-daylight-saving-time-is-in-effect-and-if-it-is-whats#answer-11888430
internals.isDstObserved = () => {

    const { stdTimezoneOffset } = internals;

    const d = new Date();
    return d.getTimezoneOffset() < stdTimezoneOffset();
};
