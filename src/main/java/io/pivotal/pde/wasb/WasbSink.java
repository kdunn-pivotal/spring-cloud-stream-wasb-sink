/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pivotal.pde.wasb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

// Include the following imports to use blob APIs.
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;


/**
 * @author Kyle Dunn
 */
@EnableBinding(Sink.class)
@EnableConfigurationProperties(WasbSinkApplicationProperties.class)
public class WasbSink {

    protected static Logger LOG = LoggerFactory.getLogger(WasbSink.class);

    @Autowired
    private WasbSinkApplicationProperties properties;

    @Autowired
    private CloudBlockBlob blob;

    @Bean
    public CloudBlockBlob getBlobService() {
        CloudBlockBlob blobService;
        
        // Define the connection-string with your values
        final String storageConnectionString =
            "DefaultEndpointsProtocol=" + this.properties.getDefaultEndpointsProtocol() +
            ";AccountName=" + this.properties.getAccountName() +
            ";AccountKey=" + this.properties.getAccountKey();

        try {
            // Setup the cloud storage account.
            CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);

            LOG.info("getBlobService() : using account {}", this.properties.getAccountName());

            // Create a blob service client
            CloudBlobClient blobClient = account.createCloudBlobClient();
            
            // Get a reference to a container
            // The container name must be lower case
            // Append a random UUID to the end of the container name so that
            // this sample can be run more than once in quick succession.
            CloudBlobContainer container = blobClient.getContainerReference(this.properties.getContainerName());

            LOG.info("getBlobService() : using container {}", this.properties.getContainerName());

            if (this.properties.getAutoCreateContainer()) {
                container.createIfNotExists();
            }            

            // Make the container public
            if (this.properties.getPublicPermission()) {
                LOG.info("getBlobService() : making container publically accessible");

                // Create a permissions object
                BlobContainerPermissions containerPermissions = new BlobContainerPermissions();

                // Include public access in the permissions object
                containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);

                // Set the permissions on the container
                container.uploadPermissions(containerPermissions);
            }

            LOG.info("getBlobService() : using blob name {}", this.properties.getBlobName());
            blobService = container.getBlockBlobReference(this.properties.getBlobName());
        } catch (Exception e) {
            // Log the stack trace.
            LOG.error("getBlobService() : {}", e);
            blobService = null;
        }
        return blobService;
    }

    @ServiceActivator(inputChannel=Sink.INPUT)
    public void pushToWasb(Message<?> message) throws MessagingException {
        try {
            // Upload the payload to the blob
            blob.uploadText(message.getPayload().toString());

        } catch (Exception e) {
            // Log the stack trace.
            LOG.error("pushToWasb() : {}", e);
        }

    }

}
