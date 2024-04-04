import { moduleId } from '../module';
import { Kafka } from 'kafkajs';
import { Logger } from 'winston';
import { Config } from '@backstage/config';
import {
  EntityProvider,
  EntityProviderConnection,
} from '@backstage/plugin-catalog-node';

export class MidgardEntityProvider implements EntityProvider {
  private connection?: EntityProviderConnection;  
  private readonly logger: Logger;
  private readonly cluster: any;
  private readonly client: any;

  constructor(opts: {
    logger: Logger;
    config: Config;
  }) {
    this.logger = opts.logger;
    const kafkaConfig: Config = opts.config?.getConfig('kafka');

    for (const clusterConfig of kafkaConfig.getConfigArray('clusters')) {
      if(clusterConfig.getString('name') === moduleId) {
        this.cluster = { clientId: kafkaConfig.getString("clientId"), brokers: clusterConfig.getStringArray('brokers') };

        for (const clientConfig of clusterConfig.getConfigArray('clients')) {
          if(clientConfig.getString('name') === this.getProviderName()) {
            this.client = { name: this.getProviderName(), topics: clientConfig.getStringArray('topics'), fromBeginning: clientConfig.getBoolean('fromBeginning')};
            break;
          }
        }

        break;
      }
    }
    
    if (!this.cluster) {
      throw new Error(`${moduleId} cluster config not found`);
    }
    
    if (!this.client) {
      throw new Error(`${this.getProviderName()} client not found`);
    }
  }

  getProviderName(): string {
    return `${moduleId}-midgard-entity-provider`;
  }

  // TODO: Investigate EventsService API once POC is done @ https://github.com/backstage/backstage/issues/20561
  /** {@inheritdoc @backstage/plugin-catalog-backend#EntityProvider.connect} */
  async connect(connection: EntityProviderConnection): Promise<void> {
    this.connection = connection;

    const kafka = new Kafka({
      clientId: this.cluster.clientId,
      brokers: this.cluster.brokers,
    })

    const consumer = kafka.consumer({ groupId: this.client.name });

    await consumer.connect()
    await consumer.subscribe({ topics: this.client.topics, fromBeginning: this.client.fromBeginning })

    // TODO: Convert topic messages into catalog entities (kind: template)
    // TODO: applyMutation(entities) and pass them to the connection for ingestion into the catalog
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        this.logger.info(JSON.stringify({
          conntion: this.connection,
          topic: topic,
          partition: partition,
          key: message.key?.toString(),
          value: message.value?.toString(),
          headers: message.headers,
        }));
      },
    })
  }
}