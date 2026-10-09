use crate::{
    change::{ChangeSelection, ChangeSelector, OutputWithoutScriptpubkey},
    selection::InputSelection,
};

pub struct SingleOutputChangeSelector;

impl ChangeSelector for SingleOutputChangeSelector {
    fn select(
        &self,
        request: InputSelection,
        dust_limit: bitcoin::Amount,
    ) -> Result<ChangeSelection, Self::Error> {
        if request.surplus < dust_limit {
            return Ok(ChangeSelection::new(vec![]));
        }
        Ok(ChangeSelection::new(vec![OutputWithoutScriptpubkey(
            request.surplus,
        )]))
    }

    type Error = ();
}

#[cfg(test)]
mod tests {
    use bitcoin::Amount;

    use super::*;

    fn selection(surplus: u64) -> InputSelection {
        InputSelection {
            selected_inputs: vec![],
            surplus: Amount::from_sat(surplus),
        }
    }

    #[test]
    fn surplus_below_the_dust_limit_creates_no_change() {
        let change = SingleOutputChangeSelector
            .select(selection(545), Amount::from_sat(546))
            .unwrap();

        assert_eq!(change, ChangeSelection::new(vec![]));
    }

    #[test]
    fn surplus_at_the_dust_limit_becomes_one_change_output() {
        let change = SingleOutputChangeSelector
            .select(selection(546), Amount::from_sat(546))
            .unwrap();

        assert_eq!(
            change,
            ChangeSelection::new(vec![OutputWithoutScriptpubkey(Amount::from_sat(546))])
        );
    }
}
