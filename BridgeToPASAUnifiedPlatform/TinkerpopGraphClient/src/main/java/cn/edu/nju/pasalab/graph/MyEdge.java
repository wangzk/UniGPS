package cn.edu.nju.pasalab.graph;

/**
 * The data structure to represent a weighted edge.
 */
final public class MyEdge {

    private String src;
    private String dst;
    private Double weight;
    public MyEdge(String src, String dst, double weight) {
        this.setSrc(src);
        this.setDst(dst);
        this.setWeight(weight);
    }

    public MyEdge(String src, String dst) {
        this.setSrc(src);
        this.setDst(dst);
        this.setWeight(1.0);
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }
}