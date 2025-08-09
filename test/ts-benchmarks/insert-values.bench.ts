import { bench } from '@ark/attest'
import type {
  InsertQueryBuilder,
  InsertResult,
  Kysely,
} from '../../dist/esm/index.js'
import type { DB } from '../typings/test-d/huge-db.test-d'

declare const kysely: Kysely<DB>

console.log('insert-values.bench.ts:\n')

let query: InsertQueryBuilder<DB, 'my_table', InsertResult>

bench.baseline(() => {
  query = kysely.insertInto('my_table')
})

bench('kysely..values(plain)', () =>
  query.values({
    col_1d726898491fbca9a8dac855d2be1be8: 1,
    col_2e66a5c7e24d1d066230f368ce8b094e: 'a',
    col_2f76db193eac6ad0f152563313673ac9: new Date(),
    col_3917508388f24a50271f7088b657123c: 'b',
    col_454ff479a3b5a9ef082d9be9ac02a6f4: 'c',
    col_4f84013a8b5e4c2b7529058c8fafcaa8: 'd',
    col_d2508118d0d39e198d1129d87d692d59: new Date(),
    col_164b7896ec8e770207febe0812c5f052: 2,
    col_286755fad04869ca523320acce0dc6a4: null,
    col_40e8a963a35b093731af6c3581d35bd2: 3,
    col_4d742b2f247bec99b41a60acbebc149a: 4,
    col_6f5e1903664b084bf6197f2b86849d5e: 5,
    col_6f7a0a5f582c69dd4c6be0a819e862cb: 'e',
    col_aa4ec89520d642ef7dfed01d47c97c02: 'f',
    col_af4e225b70a9bbd83cc3bc0e7ef24cfa: new Date(),
    id: 6,
  }),
).types([1838, 'instantiations'])
