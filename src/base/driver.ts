import { v1 as neo4j } from "neo4j-driver";
import { NEO4J_SETTINGS } from "../config";
import { Parameters } from "neo4j-driver/types/v1/statement-runner";

export enum TransactionTypes {
    READ = "READ",
    WRITE = "WRITE",
}

export class Neo4jDriver {
    private static instance: Neo4jDriver | any = null;
    public static get Instance(): Neo4jDriver {
      return this.instance || (this.instance = new this());
    }

    private driver: neo4j.Driver;

    private localSession!: neo4j.Session;
    private localTransaction!: neo4j.Transaction;

    private constructor() {
        try {
            this.driver = neo4j.driver(
                NEO4J_SETTINGS.uri,
                neo4j.auth.basic(NEO4J_SETTINGS.user, NEO4J_SETTINGS.password),
                {
                    // logging: {
                    //     level: "debug",
                    //     logger: (level, message) => {
                    //         console.log(message);
                    //     },
                    // },
                    connectionTimeout: 2000,
                    maxTransactionRetryTime: 2000,
                },
            );
        } catch (err) {
            throw err;
        }
    }

    public async openSession(type: TransactionTypes): Promise<void> {
        let session: neo4j.Session;
        if (type === TransactionTypes.READ) {
            session = this.driver.session("READ");
        } else {
            session = this.driver.session("WRITE");
        }
        this.localSession = session;
    }

    public async closeSession(): Promise<void> {
        this.localSession.close();
    }

    public async openTransaction(): Promise<void> {
        this.localTransaction = this.localSession.beginTransaction();
    }

    public async commitTransaction(): Promise<void> {
        await this.localTransaction.commit();
    }

    public async rollbackTransaction(): Promise<void> {
        await this.localTransaction.rollback();
    }

    public async runQuery(query: string, params: Parameters): Promise<neo4j.StatementResult> {
        // console.log(query);
        // console.log(params);
        const result = await this.localTransaction.run(query, params);
        return result;
    }
}
