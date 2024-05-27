package fr.formation.api_insert_data.dto;

//public class SolarWindDto {

//}

public class SolarWindDto {
    private String date;
    private Float speed;
    private Float density;
    private Float bt;
    private Float bz;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Float getSpeed() {
        return speed;
    }

    public void setSpeed(Float speed) {
        this.speed = speed;
    }

    public Float getDensity() {
        return density;
    }

    public void setDensity(Float density) {
        this.density = density;
    }

    public Float getBt() {
        return bt;
    }

    public void setBt(Float bt) {
        this.bt = bt;
    }

    public Float getBz() {
        return bz;
    }

    public void setBz(Float bz) {
        this.bz = bz;
    }
}
