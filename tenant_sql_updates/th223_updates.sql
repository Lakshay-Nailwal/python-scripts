UPDATE th223.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DELIVERY_CHALLAN_NORMAL' WHERE id IN (17,54) AND invoice_sequence_type IS NULL;
UPDATE th223.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DEBIT_NOTE_NUMBER' WHERE id IN (1116,2638) AND invoice_sequence_type IS NULL;
