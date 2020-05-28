'use strict';

const Hoek = require('@hapi/hoek');
const AWS = require('aws-sdk');
const Schmervice = require('schmervice');

// A general 'hub' type service that should also handle any particulars of working w/ the SDK
module.exports = class AwsService extends Schmervice.Service {

    getSDKServiceInstance({ sdkName, pluginOptionsName, apiVersion }) {

        const {
            region,
            accessKeyId,
            secretAccessKey
        } = this.getServiceOptions(pluginOptionsName);

        const instance = new AWS[sdkName]({
            apiVersion,
            region,
            accessKeyId,
            secretAccessKey
        });

        return instance;
    }

    getServiceOptions(awsServicePluginName, { assertBasicOptions = false } = {}) {

        const {
            [awsServicePluginName]: {
                ...scopedOptions
            } = {},
            ...rootOptions
        } = this.options || {};

        const options = { ...rootOptions, ...scopedOptions };

        const {
            region,
            accessKeyId,
            secretAccessKey
        } = options;

        if (assertBasicOptions) {
            Hoek.assert(region, `Must specify options.region or options.${awsServicePluginName}.region`);
            Hoek.assert(accessKeyId, `Must specify options.accessKeyId or options.${awsServicePluginName}.accessKeyId`);
            Hoek.assert(secretAccessKey, `Must specify options.secretAccessKey or options.${awsServicePluginName}.secretAccessKey`);
        }

        return options;
    }
};
