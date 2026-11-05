-- FR-H17: a notebook may be attached to a published share (the owner's live kernel).
-- A share that goes away (unpublished, device removed) detaches itself.
ALTER TABLE notebooks ADD COLUMN share_id TEXT REFERENCES shares(share_id) ON DELETE SET NULL;
