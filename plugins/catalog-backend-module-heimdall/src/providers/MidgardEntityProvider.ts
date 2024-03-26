import {
  EntityProvider,
  EntityProviderConnection,
} from '@backstage/plugin-catalog-node';
import { EventParams, EventsService } from '@backstage/plugin-events-node';
import { Logger } from 'winston';

export class MidgardEntityProvider implements EntityProvider {
  private connection?: EntityProviderConnection;  
  private readonly logger: Logger;
  private readonly events: EventsService;
  private readonly topics: string[];

  constructor(opts: {
    events: EventsService;
    logger: Logger;
    topics: string[];
  }) {
    this.events = opts.events;
    this.logger = opts.logger;
    this.topics = opts.topics;
  }

  getProviderName(): string {
    return `midgard-entity-provider`;
  }

  async connect(connection: EntityProviderConnection): Promise<void> {
    this.connection = connection;
  }

  async subscribe() {
    await this.events.subscribe({
      id: 'MidgardEntityProvider',
      topics: this.topics,
      onEvent: async (params: EventParams): Promise<void> => {
        if (!this.connection) {
          throw new Error('Connection not initialized');
        }

        this.logger.info(
          `onEvent: topic=${params.topic}, metadata=${JSON.stringify(
            params.metadata,
          )}, payload=${JSON.stringify(params.eventPayload)}`,
        );

        // TODO: Investigate EventsService API and figure out if there is a way to connect it with kafka using the existing kafka-plugin logic
        // TODO: Fetch events from Midgard Kafka Broker either indirectly via EventsService or direectly via a KafkaClient
        // TODO: Convert topic messages into catalog entities (kind: template)
        // TODO: applyMutation(entities) and pass them to the connection for ingestion into the catalog
      },
    });
  }
}