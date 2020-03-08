import java.io.Serializable;

public class Item implements Serializable {
    public int Count;
    public int Sum;
    public Item(int count,int sum){
        Count=count;
        Sum=sum;
    }
    @Override
    public String toString(){
        return String.format ("%.2f",Sum*1.0/Count)+","+Sum;
    }
}