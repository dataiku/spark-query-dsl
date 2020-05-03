/*
 * Copyright 2020 Adrien Lavoillotte, Dataiku
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataiku.spark.query

import org.apache.spark.sql.DataFrame


// Provide code-compatibility with Spark 1.6 and older 2.x
// Some operations will fail at runtime though
package object compat {
    import scala.language.implicitConversions

    class AugmentedDF(df: DataFrame) {
        def union(o: DataFrame): DataFrame = throw new NotImplementedError("union needs spark 2.0")

        def unionByName(o: DataFrame): DataFrame =  // From Spark 2.3
            if (o.columns.sameElements(df.columns)) df.union(o) // no difference in columns
            else                                    df.union(o.select(df.columns.head, df.columns.tail :_*))

        def crossJoin(o: DataFrame): DataFrame = df.join(o) // would throw at runtime in spark 2
    }

    implicit def augmentDf(df: DataFrame): AugmentedDF = new AugmentedDF(df)
}
