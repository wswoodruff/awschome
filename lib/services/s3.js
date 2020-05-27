'use strict';

const Hoek = require('@hapi/hoek');

const Schmervice = require('schmervice');

const S3_API_VERSION = '2006-03-01';

module.exports = class S3Service extends Schmervice.Service {

    constructor(server, options) {

        super(server, options);

        const { awsService } = server.services();

        const { bucket, s3Bucket } = awsService.getServiceOptions('s3');

        this.bucket = s3Bucket || bucket;

        // No bucket, no service
        if (!this.bucket) {
            return;
        }

        this.s3 = awsService.getSDKServiceInstance({
            sdkName: 'S3',
            pluginOptionsName: 's3',
            apiVersion: S3_API_VERSION
        });
    }

    async upload({ key, body, options = {} }) {

        this._ensureConfiguration();

        return await this.s3.upload({
            Key: key,
            Bucket: this.bucket,
            Body: body,
            ...options
        }).promise();
    }

    async getMetadata(objectId) {

        this._ensureConfiguration();

        return await this.s3.headObject({
            Key: objectId,
            Bucket: this.bucket
        }).promise();
    }

    getStream(objectId) {

        this._ensureConfiguration();

        return this.s3.getObject({
            Key: objectId,
            Bucket: this.bucket
        }).createReadStream();
    }

    async deleteById(objectId) {

        this._ensureConfiguration();

        return await this.s3.deleteObject({
            Key: objectId,
            Bucket: this.bucket
        }).promise();
    }

    // NOTE these default to expire in 15 minutes
    getSignedUrl(objectId) {

        this._ensureConfiguration();

        return this.s3.getSignedUrl('getObject', {
            Key: objectId,
            Bucket: this.bucket
        }).promise();
    }

    _ensureConfiguration() {

        Hoek.assert(this.s3 && this.bucket, 's3Service is improperly configured');
    }
};
