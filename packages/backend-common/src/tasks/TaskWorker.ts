/*
 * Copyright 2021 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Knex } from 'knex';
import { Duration } from 'luxon';
import { Logger } from 'winston';
import { DbTasksRow } from '../database/tables';
import { CancelToken } from './CancelToken';
import { TaskSettingsV1, taskSettingsV1Schema } from './types';
import { nowPlus } from './util';

const WORK_CHECK_FREQUENCY = Duration.fromObject({ seconds: 5 });

/**
 * Performs the actual work of a task.
 *
 * @private
 */
export class TaskWorker {
  private readonly taskId: string;
  private readonly fn: () => void | Promise<void>;
  private readonly knex: Knex;
  private readonly logger: Logger;
  private readonly cancelToken: CancelToken;

  constructor(
    taskId: string,
    fn: () => void | Promise<void>,
    knex: Knex,
    logger: Logger,
  ) {
    this.taskId = taskId;
    this.fn = fn;
    this.knex = knex;
    this.logger = logger;
    this.cancelToken = CancelToken.create();
  }

  async start(settings: TaskSettingsV1) {
    await this.persistTask(settings);

    this.logger.debug(
      `Task worker starting: ${this.taskId}, ${JSON.stringify(settings)}`,
    );

    this.runLoop().then(
      () => this.logger.debug(`Task worker finished: ${this.taskId}`),
      e => this.logger.warn(`Task worker failed unexpectedly, ${e}`),
    );
  }

  stop() {
    this.cancelToken.cancel();
  }

  private async runLoop() {
    while (!this.cancelToken.isCancelled) {
      const runResult = await this.runOnce();
      if (runResult.result === 'abort') {
        return;
      }

      await this.sleep(WORK_CHECK_FREQUENCY);
    }
  }

  private async runOnce(): Promise<
    | { result: 'not ready yet' }
    | { result: 'abort' }
    | { result: 'failed' }
    | { result: 'completed' }
  > {
    const findResult = await this.findReadyTask();
    if (
      findResult.result === 'not ready yet' ||
      findResult.result === 'abort'
    ) {
      return findResult;
    }

    return { result: 'completed' };
  }

  /**
   * Sleeps for the given duration, but aborts sooner if the cancel token
   * triggers.
   */
  private async sleep(duration: Duration) {
    await Promise.race([
      new Promise(resolve => setTimeout(resolve, duration.as('milliseconds'))),
      this.cancelToken.promise,
    ]);
  }

  /**
   * Perform the initial store of the task info
   */
  private async persistTask(settings: TaskSettingsV1) {
    // Perform an initial parse to ensure that we will definitely be able to
    // read it back again.
    taskSettingsV1Schema.parse(settings);

    const initialDelay = settings.initialDelayDuration
      ? Duration.fromISO(settings.initialDelayDuration)
      : undefined;

    // It's OK if the task already exists; if it does, just replace its
    // settings with the new value and start the loop as usual.
    try {
      await this.knex<DbTasksRow>('backstage_backend_common__tasks')
        .insert({
          id: this.taskId,
          settings_json: JSON.stringify(settings),
          next_run_start_at: nowPlus(initialDelay, this.knex),
        })
        .onConflict('id')
        .merge(['settings_json']);
    } catch (e) {
      throw new Error(`Failed to persist task, ${e}`);
    }
  }

  /**
   * Check if the task is ready to run
   */
  private async findReadyTask(): Promise<
    | { result: 'not ready yet' }
    | { result: 'abort' }
    | { result: 'ready'; settings: TaskSettingsV1 }
  > {
    let settingsJson: string;
    try {
      const rows = await this.knex<DbTasksRow>(
        'backstage_backend_common__tasks',
      )
        .where('id', '=', this.taskId)
        .where('next_run_start_at', '<', this.knex.fn.now())
        .whereNull('current_run_ticket')
        .select(['settings_json']);

      if (!rows.length) {
        return { result: 'not ready yet' };
      }

      settingsJson = rows[0].settings_json;
    } catch (e) {
      this.logger.warn(
        `Task worker failed to fetch task information; will try again later, ${e}`,
      );
      return { result: 'not ready yet' };
    }

    let settings: TaskSettingsV1;
    try {
      settings = taskSettingsV1Schema.parse(JSON.parse(settingsJson));
    } catch (e) {
      this.logger.info(
        `No longer able to parse task settings; aborting and assuming that a newer version of the task has been issued and being handled by other workers, ${e}`,
      );
      return { result: 'abort' };
    }

    return { result: 'ready', settings };
  }
}
