import java.io.Serializable;

public class POJO implements Serializable {

    private static final long serialVersionUID = 1L;

    private String title;
    private String company;
    private String location;
    private String type;
    private String level;
    private String years_of_experience;
    private String country;
    private String skills;

    public POJO(){

    }
    public POJO(String title,String company,String location,String type,String level,String years_of_experience,String country,String skills){
        this.title=title;
        this.company=company;
        this.location=location;
        this.type=type;
        this.level=level;
        this.years_of_experience=years_of_experience;
        this.country=country;
        this.skills=skills;
    }


    public void setTitle(String title) {
        this.title = title;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setYears_of_experience(String years_of_experience) {
        this.years_of_experience = years_of_experience;
    }


    public void setSkills(String skills) {
        this.skills = skills;
    }


    public String getTitle() {
        return title;
    }

    public String getCompany() {
        return company;
    }

    public String getLocation() {
        return location;
    }

    public String getType() {
        return type;
    }

    public String getLevel() {
        return level;
    }

    public String getYears_of_experience() {
        return years_of_experience;
    }

    public String getCountry() {
        return country;
    }
    public String getSkills() {
        return skills;
    }


}

