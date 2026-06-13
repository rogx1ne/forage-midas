package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Optional;

@RestController
public class BalanceController {

    @Autowired
    private UserRepository userRepository;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        // 1. Look up user by ID (returns an Optional wrapper)
        Optional<UserRecord> userOpt = userRepository.findById(userId);
        
        // 2. If the user is present in the database, extract them and return their balance
        if (userOpt.isPresent()) {
            UserRecord user = userOpt.get();
            return new Balance(user.getBalance());
        }
        
        // 3. If user doesn't exist, return a Balance of 0
        return new Balance((float) 0.0);
    }
}