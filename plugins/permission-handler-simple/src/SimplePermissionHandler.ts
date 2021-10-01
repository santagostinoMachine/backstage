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

import { BackstageIdentity } from '@backstage/plugin-auth-backend';
import {
  AuthorizeFiltersResponse,
  AuthorizeResult,
  AuthorizeRequestContext,
  AuthorizeRequest,
  AuthorizeResponse,
} from '@backstage/permission-common';
import { PermissionHandler } from '@backstage/plugin-permission-backend';
import {
  CatalogPermission,
  EntityName,
  hasAnnotation,
  isEntityOwner,
  isEntityKind,
  parseEntityRef,
} from '@backstage/catalog-model';

export class SimplePermissionHandler implements PermissionHandler {
  async handle(
    request: AuthorizeRequest<AuthorizeRequestContext>,
    identity?: BackstageIdentity,
  ): Promise<AuthorizeResponse> {
    if (identity) {
      return {
        result: AuthorizeResult.ALLOW,
      };
    }

    if (request.permission.isRead) {
      return {
        result: AuthorizeResult.ALLOW,
      };
    }

    return {
      result: AuthorizeResult.DENY,
    };
  }

  async authorizeFilters(
    request: AuthorizeRequest<AuthorizeRequestContext>,
    identity?: BackstageIdentity,
  ): Promise<AuthorizeFiltersResponse> {
    if (CatalogPermission.includes(request.permission)) {
      if (!identity) {
        return {
          result: AuthorizeResult.DENY,
        };
      }

      return {
        result: AuthorizeResult.MAYBE,
        conditions: {
          anyOf: [
            {
              allOf: [
                // TODO(authorization-framework) eventually all the claims
                // should be pulled off the token and used to evaluate
                // transitive ownership (I own anything owned by my user
                // or any of the groups I'm in).
                isEntityOwner([
                  parseEntityRef(identity?.id ?? '', {
                    defaultKind: 'user',
                    defaultNamespace: 'default',
                  }) as EntityName,
                ]),
                hasAnnotation('backstage.io/view-url'),
              ],
            },
            {
              allOf: [isEntityKind(['template'])],
            },
          ],
        },
      };
    }

    return {
      result: AuthorizeResult.DENY,
    };
  }
}
