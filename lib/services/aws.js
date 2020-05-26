'use strict';

const Hoek = require('@hapi/hoek');
const AWS = require('aws-sdk');
const Schmervice = require('schmervice');

// A general 'hub' type service that should also handle any particulars of working w/ the SDK
module.exports = class AwsService extends Schmervice.Service {

    getSDKServiceInstance({ sdkName, pluginOptionsName, apiVersion, autoPromises = true }) {

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

        if (autoPromises) {
            // Monkeypatch all funcs to run .promise() where available
            Object.entries(instance).forEach(([key, val]) => {

                if (typeof val === 'function') {
                    instance[key] = async (...args) => {

                        const res = val(...args);

                        if (typeof res.promise === 'function') {
                            return await res.promise();
                        }
                    };
                }
            });
        }

        return instance;
    }

    getServiceOptions(awsServicePluginName) {

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

        Hoek.assert(region, `Must specify options.region or options.${awsServicePluginName}.region`);
        Hoek.assert(accessKeyId, `Must specify options.accessKeyId or options.${awsServicePluginName}.accessKeyId`);
        Hoek.assert(secretAccessKey, `Must specify options.secretAccessKey or options.${awsServicePluginName}.secretAccessKey`);

        return options;
    }
};
