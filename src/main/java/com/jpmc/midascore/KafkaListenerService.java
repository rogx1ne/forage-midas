package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaListenerService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    private final RestTemplate restTemplate = new RestTemplate();
    private final String INCENTIVE_API_URL = "http://localhost:8080/incentive";

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            if (sender.getBalance() >= transaction.getAmount()) {
                
                // 1. Call the external REST API to fetch the incentive payout
                Incentive incentiveResponse = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
                double incentiveAmount = (incentiveResponse != null) ? incentiveResponse.getAmount() : 0.0;

                // 2. Compute the updated balances (recipient gets the bonus!)
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance((float) (recipient.getBalance() + transaction.getAmount() + incentiveAmount));

                // 3. Persist the state alterations down to H2 storage tables
                userRepository.save(sender);
                userRepository.save(recipient);

                // 4. Populate and record the execution logs
                TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
                record.setIncentive(incentiveAmount); // Save the incentive payload inside the record log
                transactionRecordRepository.save(record);
            }
        }
    }
}