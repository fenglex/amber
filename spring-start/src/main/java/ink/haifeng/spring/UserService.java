package ink.haifeng.spring;


import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {




    @Transactional(propagation = Propagation.NESTED)
    public void insertUser(){

    }
}
