package org.apache.calcite.adapter.hustle.operators;

import java.util.*;

public class HustleLogicalPlanNode {
    public HustleOperator operator;
    public ArrayList<HustleLogicalPlanNode> children;

    public HustleLogicalPlanNode() {
        children = new ArrayList<>();
    }
    public HustleLogicalPlanNode(HustleOperator operator, ArrayList<HustleLogicalPlanNode> children) {
        this.operator = operator;
        this.children = children;
    }
}
