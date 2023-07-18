package calculateavarage;

public class ClassDegree {
    private String className;
    private long timeStamp;
    private double average;

    public double getAvarage() {
        return average;
    }

    public void setAvarage(double avarage) {
        this.average = avarage;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    @Override
    public String toString() {
        return "ClassDegree{" +
                "className='" + className + '\'' +
                ", timeStamp=" + timeStamp +
                ", average=" + average +
                '}';
    }
}
