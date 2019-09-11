package org.apache.calcite.adapter.hustle.operators;

public class HustleProject implements HustleOperator {
    public String name = "Project";
    public String toJson() {
        return "customProject";
    }
}
