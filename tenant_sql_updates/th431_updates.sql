UPDATE th431.purchase_issue SET updated_on = NOW(), invoice_sequence_type = 'DELIVERY_CHALLAN_NORMAL' WHERE id IN (93) AND invoice_sequence_type IS NULL;
