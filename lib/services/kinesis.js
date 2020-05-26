'use strict';

const Hoek = require('@hapi/hoek');

const Schmervice = require('schmervice');

const KINESIS_API_VERSION = '2013-12-02';

const internals = {};

module.exports = class KinesisService extends Schmervice.Service {

    constructor(server, options) {

        super(server, options);

        const { awsService } = server.services();

        const { streamName, kinesisStreamName } = awsService.getServiceOptions('kinesis');

        this.streamName = streamName || kinesisStreamName;

        console.log('this.streamName', this.streamName);

        // No streamName, no service
        if (!this.streamName) {
            return;
        }

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
            Records: records.map(this.formatRecord),
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
                console.log('Sent', testRecord);
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

        this._ensureConfiguration();

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
            source: 'kinene',
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
