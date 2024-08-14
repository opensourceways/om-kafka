/* This project is licensed under the Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL v2.
 You may obtain a copy of Mulan PSL v2 at:
     http://license.coscl.org.cn/MulanPSL2
 THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
 PURPOSE.
 See the Mulan PSL v2 for more details.
 Create: 2024
*/

package om.constant;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 性能数据常量.
 */
public final class PerfConstant {
    /**
     * 禁止构造方法.
     */
    private PerfConstant() {
        throw new AssertionError("Constant class cannot be instantiated.");
    }

    /**
     * 集群性能指标.
     */
    public static final List<Integer> CLUSTER_METRIC = Collections.unmodifiableList(new ArrayList<Integer>() {
        {
            // npu算力
            add(1001);
        }
    });

    /**
     * 所有支持的性能指标.
     */
    public static final Map<Integer, List<Integer>>  PERFORMANCE_METRIC = Collections.unmodifiableMap(
            new HashMap<Integer, List<Integer>>() {
        {
            // 集群
            put(10, CLUSTER_METRIC);
        }
    });
}
