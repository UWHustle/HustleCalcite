package org.apache.calcite.adapter.hustle;

import com.google.gson.*;
import org.apache.calcite.adapter.hustle.operators.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.*;

public class HustleUtils {

    public static String toHustleLogicalPlan(RelRoot relRoot){
        RelNode node = relRoot.rel;
        Gson gson = new Gson();
        System.out.println(gson.toJson(convertNode(node)));
        return "";
//      return convertPlan(node, 0);
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static HustleLogicalPlanNode convertNode(RelNode node) {
        HustleLogicalPlanNode hustleNode = new HustleLogicalPlanNode();
        hustleNode.operator = convertOperator2(node);

        for(RelNode n : node.getInputs()) {
            HustleLogicalPlanNode newn = convertNode(n);
            hustleNode.children.add(newn);
        }
        return hustleNode;
    }

    private static HustleOperator convertOperator2(RelNode node) {
        if( node instanceof Project) {
            return convertOperator2((Project) node);
        } else if( node instanceof Filter) {
            return convertOperator2((Filter) node);
        } else if( node instanceof Sort) {
            return convertOperator2((Sort) node);
        } else {
            return null;
        }
    }

    private static HustleOperator convertOperator2(Project project){
//        System.out.println("project");
        return new HustleProject();
    }

    private static HustleOperator convertOperator2(Filter filter){
//        System.out.println("filter");
        return new HustleTableFilter();
    }

    private static HustleOperator convertOperator2(TableScan sort){
//        System.out.println("TableScan\n");
        return new HustleTableScan();
    }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static String convertPlan(RelNode node, int level) {
        String plan = "";
        String indent = "";
        for(int i = 0 ; i < level; ++i) {
            indent +="  ";
        }
        plan += indent + convertOperator(node) + "\n";
        for(RelNode n : node.getInputs()) {
            plan += convertPlan(n, level+1);
        }
        return "";
//        return pl
    }

    private static String convertOperator(RelNode node) {
        if( node instanceof Project) {
            return convertOperator((Project) node);
        } else if( node instanceof Filter) {
            return convertOperator((Filter) node);
        } else if( node instanceof Sort) {
            return convertOperator((Sort) node);
        } else if( node instanceof Aggregate) {
            return convertOperator((Aggregate) node);
        } else if( node instanceof Join) {
            return convertOperator((Join) node);
        } else if( node instanceof TableScan) {
            return convertOperator((TableScan) node);
        } else {
            return "WHAT IS THIS OPERATOR?";
        }
    }

    private static String convertOperator(Project project){
//        System.out.println("project");
        return "project";
    }

    private static String convertOperator(Filter filter){
//        System.out.println("filter");
        return "filter";
    }

    private static String convertOperator(Sort sort){
        System.out.println("sort\n");
        return "sort";
    }

    private static String convertOperator(Aggregate sort){
//        System.out.println("aggregate\n");
        return "aggregate";
    }

    private static String convertOperator(Join sort){
//        System.out.println("join\n");
        return "join";
    }

    private static String convertOperator(TableScan sort){
//        System.out.println("TableScan\n");
        return "table";
    }
}
